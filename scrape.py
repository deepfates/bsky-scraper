from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
import json
import time
import argparse
from datetime import datetime
import multiprocessing
import sys
import signal

def worker_process(queue, output_file, verbose, post_count, lock, stop_event):
    """Worker process that processes messages from the queue"""
    # Each worker needs its own resolver and cache
    resolver = IdResolver(cache=DidInMemoryCache())
    
    while not stop_event.is_set():
        try:
            message = queue.get(timeout=1)
            process_message(message, resolver, output_file, verbose, post_count, lock)
        except multiprocessing.queues.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")

def process_message(message, resolver, output_file, verbose, post_count, lock):
    """Process a single message"""
    try:
        commit = parse_subscribe_repos_message(message)
        if not hasattr(commit, 'ops'):
            return

        for op in commit.ops:
            if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                process_post(commit, op, resolver, output_file, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing message: {e}")

def process_post(commit, op, resolver, output_file, verbose, post_count, lock):
    """Process a single post operation"""
    try:
        author_handle = resolve_author_handle(commit.repo, resolver)
        car = CAR.from_bytes(commit.blocks)
        for record in car.blocks.values():
            if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                post_data = extract_post_data(record, commit.repo, op.path, author_handle)
                save_post_data(post_data, output_file, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing record: {e}")

def resolve_author_handle(repo, resolver):
    """Resolve the author handle from the DID"""
    try:
        resolved_info = resolver.did.resolve(repo)
        return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
    except Exception as e:
        print(f"Could not resolve handle for {repo}: {e}")
        return repo  # Fallback to DID

def extract_post_data(record, repo, path, author_handle):
    """Extract post data from a record"""
    has_images = check_for_images(record)
    reply_to = get_reply_to(record)
    return {
        'text': record.get('text', ''),
        'created_at': record.get('createdAt', ''),
        'author': author_handle,
        'uri': f'at://{repo}/{path}',
        'has_images': has_images,
        'reply_to': reply_to
    }

def check_for_images(record):
    """Check if the post has images"""
    embed = record.get('embed', {})
    return (
        embed.get('$type') == 'app.bsky.embed.images' or
        (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
    )

def get_reply_to(record):
    """Get the URI of the post being replied to"""
    reply_ref = record.get('reply', {})
    return reply_ref.get('parent', {}).get('uri')

def save_post_data(post_data, output_file, verbose, post_count, lock):
    """Save post data to the output file"""
    with lock:
        with open(output_file, 'a') as f:
            json.dump(post_data, f)
            f.write('\n')
        with post_count.get_lock():
            post_count.value += 1
    if verbose:
        print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")

def client_process(queue, stop_event):
    """Process that runs the client and puts messages into the queue"""
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

class FirehoseScraper:
    def __init__(self, output_file="bluesky_posts.jsonl", verbose=False, num_workers=4):
        self.output_file = output_file
        self.post_count = multiprocessing.Value('i', 0)
        self.start_time = None
        self.verbose = verbose
        self.queue = multiprocessing.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.stop_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()  # For thread-safe file writing
        self.client_process = None

    def start_collection(self, duration_seconds=None, post_limit=None):
        """Start collecting posts from the firehose"""
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        # Start worker processes
        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(self.queue, self.output_file, self.verbose, self.post_count, self.lock, self.stop_event)
            )
            p.start()
            self.workers.append(p)

        # Start the client in a separate process
        self.client_process = multiprocessing.Process(
            target=client_process,
            args=(self.queue, self.stop_event)
        )
        self.client_process.start()

        # Handle KeyboardInterrupt in main process
        def signal_handler(sig, frame):
            print("\nCollection stopped by user.")
            self._stop_collection()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Monitor the collection
        try:
            while self.client_process.is_alive():
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
        except Exception as e:
            print(f"\nError: {e}")
            self._stop_collection()

        self._stop_collection()

    def _stop_collection(self):
        """Stop the collection and print summary"""
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.client_process and self.client_process.is_alive():
            self.client_process.terminate()
            self.client_process.join()

        # Wait for all worker processes to finish
        for p in self.workers:
            if p.is_alive():
                p.terminate()
            p.join()

        elapsed = time.time() - self.start_time
        rate = self.post_count.value / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count.value} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect posts from the Bluesky firehose')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--time', type=int, help='Collection duration in seconds')
    group.add_argument('-n', '--number', type=int, help='Number of posts to collect')
    parser.add_argument('-o', '--output', type=str,
                       default=f"bluesky_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
                       help='Output file path (default: bluesky_posts_TIMESTAMP.jsonl)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Print each post as it is collected')
    parser.add_argument('-w', '--workers', type=int, default=4,
                       help='Number of worker processes (default: 4)')

    args = parser.parse_args()

    archiver = FirehoseScraper(output_file=args.output, verbose=args.verbose, num_workers=args.workers)
    archiver.start_collection(duration_seconds=args.time, post_limit=args.number)
