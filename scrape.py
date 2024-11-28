import sqlite3
import os
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
import time
import argparse
from datetime import datetime
import multiprocessing
import sys
import signal

class FirehoseScraper:
    def __init__(self, database_path="bluesky_posts.db", verbose=False, num_workers=4):
        """
        Initialize the scraper with a SQLite database connection.
        
        Args:
            database_path (str): Path to the SQLite database file
            verbose (bool): Enable detailed logging of collected posts
            num_workers (int): Number of worker processes for parallel processing
        """
        # Firehose client setup
        self.client = FirehoseSubscribeReposClient()
        self.database_path = database_path
        self.verbose = verbose
        self.num_workers = num_workers
        self.post_count = multiprocessing.Value('i', 0)
        self.start_time = None
        self.queue = multiprocessing.Queue()
        self.workers = []
        self.stop_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()  # For thread-safe database access
        self.client_proc = None

        # Initialize database connection
        self.conn = sqlite3.connect(self.database_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._setup_database()

        # DID resolution
        self.cache = DidInMemoryCache() 
        self.resolver = IdResolver(cache=self.cache)

    def _setup_database(self):
        """
        Create database tables for storing Bluesky posts and related information.
        Includes indexes for efficient querying.
        """
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

    def start_collection(self, duration_seconds=None, post_limit=None):
        """Start collecting posts from the firehose"""
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        # Start worker processes
        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=self.worker_process,
                args=(self.queue, self.post_count, self.lock, self.stop_event)
            )
            p.start()
            self.workers.append(p)

        # Setup signal handler for graceful shutdown
        def signal_handler(sig, frame):
            print("\nCollection stopped by user.")
            self._stop_collection()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Start the client process
        self.client_proc = multiprocessing.Process(
            target=self.client_process,
            args=(self.queue, self.stop_event)
        )
        self.client_proc.start()

        # Monitor the collection
        try:
            while True:
                if self.stop_event.is_set():
                    break
                if duration_seconds and time.time() > end_time:
                    print("\nTime limit reached.")
                    self._stop_collection()
                    break
                elif post_limit and self.post_count.value >= post_limit:
                    print("\nPost limit reached.")
                    self._stop_collection()
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nCollection interrupted by user.")
            self._stop_collection()

    def worker_process(self, queue, post_count, lock, stop_event):
        while not stop_event.is_set():
            try:
                message = queue.get(timeout=1)
                self.process_message(message, lock)
                with post_count.get_lock():
                    post_count.value += 1
            except multiprocessing.queues.Empty:
                continue
            except Exception as e:
                print(f"Worker error: {e}")

    def client_process(self, queue, stop_event):
        client = FirehoseSubscribeReposClient()

        def message_handler(message):
            if stop_event.is_set():
                client.stop()
                return
            queue.put(message)

        try:
            client.start(message_handler)
        except Exception as e:
            if not stop_event.is_set():
                print(f"Client process error: {e}")

    def process_message(self, message, lock):
        """Process a single message from the firehose"""
        try:
            commit = parse_subscribe_repos_message(message)
            if not hasattr(commit, 'ops'):
                return

            for op in commit.ops:
                if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                    self._process_post(commit, op, lock)

        except Exception as e:
            print(f"Error processing message: {e}")

    def _process_post(self, commit, op, lock):
        """Process a single post operation and save to SQLite"""
        try:
            author_handle = self._resolve_author_handle(commit.repo)
            car = CAR.from_bytes(commit.blocks)
            for record in car.blocks.values():
                if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                    post_data = self._extract_post_data(record, commit.repo, op.path, author_handle)
                    self._save_post_data(post_data, record, lock)
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

    def _save_post_data(self, post_data, raw_record, lock):
        """Save post data to SQLite database"""
        try:
            with lock:
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
            
            if self.verbose:
                print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")
        
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    def _stop_collection(self):
        """Stop the collection, print summary, and close database connection"""
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.client_proc and self.client_proc.is_alive():
            self.client_proc.terminate()
            self.client_proc.join()

        for p in self.workers:
            if p.is_alive():
                p.terminate()
            p.join()

        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.post_count.value / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count.value} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.database_path}")

        # Close database connection
        self.conn.close()

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