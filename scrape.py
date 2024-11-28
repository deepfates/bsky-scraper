import sqlite3
import os
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
import time
import argparse
from datetime import datetime

class FirehoseScraper:
    def __init__(self, database_path="bluesky_posts.db", verbose=False):
        """
        Initialize the scraper with a SQLite database connection.
        
        Args:
            database_path (str): Path to the SQLite database file
            verbose (bool): Enable detailed logging of collected posts
        """
        # Firehose client setup
        self.client = FirehoseSubscribeReposClient()
        self.database_path = database_path
        
        # Database connection and cursor
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()
        
        # Create tables if they don't exist
        self._create_tables()
        
        # Additional tracking variables
        self.post_count = 0
        self.start_time = None
        
        # DID resolution
        self.cache = DidInMemoryCache() 
        self.resolver = IdResolver(cache=self.cache)
        
        # Verbosity flag
        self.verbose = verbose

    def _create_tables(self):
        """
        Create database tables for storing Bluesky posts and related information.
        Includes indexes for efficient querying.
        """
        # Posts table with comprehensive schema
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uri TEXT UNIQUE,
                author TEXT,
                text TEXT,
                created_at DATETIME,
                has_images BOOLEAN,
                reply_to TEXT,
                raw_record TEXT,
                collected_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes to improve query performance
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_author ON posts(author)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON posts(created_at)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_has_images ON posts(has_images)')
        
        # Commit the table creation
        self.conn.commit()

    def process_message(self, message) -> None:
        """Process a single message from the firehose"""
        try:
            commit = parse_subscribe_repos_message(message)
            if not hasattr(commit, 'ops'):
                return

            for op in commit.ops:
                if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                    self._process_post(commit, op)

        except Exception as e:
            print(f"Error processing message: {e}")

    def _process_post(self, commit, op):
        """Process a single post operation and save to SQLite"""
        try:
            author_handle = self._resolve_author_handle(commit.repo)
            car = CAR.from_bytes(commit.blocks)
            for record in car.blocks.values():
                if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                    post_data = self._extract_post_data(record, commit.repo, op.path, author_handle)
                    self._save_post_data(post_data, record)
        except Exception as e:
            print(f"Error processing record: {e}")

    def _resolve_author_handle(self, repo):
        """Resolve the author handle from the DID"""
        try:
            resolved_info = self.resolver.did.resolve(repo)
            return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
        except Exception as e:
            print(f"Could not resolve handle for {repo}: {e}")
            return repo  # Fallback to DID

    def _extract_post_data(self, record, repo, path, author_handle):
        """Extract post data from a record"""
        has_images = self._check_for_images(record)
        reply_to = self._get_reply_to(record)
        return {
            'text': record.get('text', ''),
            'created_at': record.get('createdAt', ''),
            'author': author_handle,
            'uri': f'at://{repo}/{path}',
            'has_images': has_images,
            'reply_to': reply_to
        }

    def _check_for_images(self, record):
        """Check if the post has images"""
        embed = record.get('embed', {})
        return (
            embed.get('$type') == 'app.bsky.embed.images' or
            (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
        )

    def _get_reply_to(self, record):
        """Get the URI of the post being replied to"""
        reply_ref = record.get('reply', {})
        return reply_ref.get('parent', {}).get('uri')

    def _save_post_data(self, post_data, raw_record):
        """Save post data to SQLite database"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO posts 
                (uri, author, text, created_at, has_images, reply_to, raw_record) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                post_data['uri'], 
                post_data['author'], 
                post_data['text'], 
                post_data['created_at'], 
                post_data['has_images'], 
                post_data['reply_to'], 
                str(raw_record)  # Convert the entire raw record to a string
            ))
            self.conn.commit()
            
            self.post_count += 1
            if self.verbose:
                print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")
        
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    def start_collection(self, duration_seconds=None, post_limit=None):
        """Start collecting posts from the firehose"""
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        def message_handler(message):
            if duration_seconds and time.time() > end_time:
                self._stop_collection()
            elif post_limit and self.post_count >= post_limit:
                self._stop_collection()
            else:
                self.process_message(message)

        try:
            while True:
                try:
                    self.client.start(message_handler)
                except Exception as e:
                    error_details = f"{type(e).__name__}: {str(e)}" if str(e) else f"{type(e).__name__}"
                    print(f"\nConnection error: {error_details}")

        except KeyboardInterrupt:
            print("\nCollection stopped by user.")
            self._stop_collection()

    def _stop_collection(self):
        """Stop the collection, print summary, and close database connection"""
        elapsed = time.time() - self.start_time
        rate = self.post_count / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.database_path}")
        
        # Close database connection
        self.conn.close()
        self.client.stop()

    def query_posts(self, limit=10, filter_condition=None):
        """
        Query posts from the database with optional filtering.
        
        Args:
            limit (int): Maximum number of posts to retrieve
            filter_condition (str, optional): SQL WHERE clause for filtering
        
        Returns:
            list: List of posts matching the query
        """
        query = "SELECT * FROM posts"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += f" LIMIT {limit}"
        
        self.cursor.execute(query)
        return self.cursor.fetchall()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect posts from the Bluesky firehose')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--time', type=int, help='Collection duration in seconds')
    group.add_argument('-n', '--number', type=int, help='Number of posts to collect')
    parser.add_argument('-o', '--output', type=str, 
                       default=f"bluesky_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                       help='Output SQLite database path')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Print each post as it is collected')

    args = parser.parse_args()
    
    archiver = FirehoseScraper(database_path=args.output, verbose=args.verbose)
    archiver.start_collection(duration_seconds=args.time, post_limit=args.number)