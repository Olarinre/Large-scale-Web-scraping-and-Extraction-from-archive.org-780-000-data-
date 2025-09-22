import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

baseurl = "https://archive.org"

# List of proxies (add your own proxies here)
PROXIES = [
    "http://lrhapelm:4b6xk0ttj9oh@142.147.128.250:6750/",
    "http://lrhapelm:4b6xk0ttj9oh@38.153.148.204:5475/",
    "http://lrhapelm:4b6xk0ttj9oh@82.26.208.215:5522/",
    "http://lrhapelm:4b6xk0ttj9oh@192.186.151.16:8517/",
    "http://lrhapelm:4b6xk0ttj9oh@192.186.172.130:9130/",
    "http://lrhapelm:4b6xk0ttj9oh@86.38.236.165:6449/",
    "http://lrhapelm:4b6xk0ttj9oh@142.147.131.242:6142/",
    "http://lrhapelm:4b6xk0ttj9oh@198.89.123.154:6696/",
    "http://lrhapelm:4b6xk0ttj9oh@45.249.59.230:6206/",
    'http://brd-customer-hl_5fd52255-zone-datacenter_proxy3:f8wflj43xbp5@brd.superproxy.io:33335',
    'http://brd-customer-hl_5fd52255-zone-isp_proxy2:yp4vahdrtdn0@brd.superproxy.io:33335',
    'http://brd-customer-hl_5fd52255-zone-isp_proxy2-country-ch:yp4vahdrtdn0@brd.superproxy.io:33335'
    #'http://brd-customer-hl_5fd52255-zone-isp_proxy1:ttxz0g85p0da@brd.superproxy.io:33335',
    #'http://brd-customer-hl_5fd52255-zone-datacenter_proxy1:z6m1uccmhk25@brd.superproxy.io:33335',
    #'http://brd-customer-hl_5fd52255-zone-datacenter_proxy2:4z3r3fgk3ipp@brd.superproxy.io:33335'
]

# If no proxies provided, use None (direct connection)
USE_PROXIES = len(PROXIES) > 0

def get_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_random_proxy():
    """Get a random proxy from the list"""
    if not USE_PROXIES:
        return None
    return random.choice(PROXIES)

def save_pageurl(url, path):
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Get a session and proxy
        session = get_session()
        proxy = get_random_proxy()
        
        # Configure request parameters
        request_params = {
            'timeout': 50,
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        }
        
        if proxy:
            request_params['proxies'] = {
                'http': proxy,
                'https': proxy
            }
            print(f"Using proxy: {proxy}")
        
        # Fetch and save the HTML content of a page
        response = session.get(url, **request_params)
        response.raise_for_status()
        
        with open(path, "w", encoding="utf-8") as file:
            file.write(response.text)
        print(f"Saved: {path}")
        return True
        
    except requests.RequestException as e:
        print(f"Failed to retrieve {url}: {e}")
        # Rotate proxy if this one failed
        if USE_PROXIES and proxy in PROXIES:
            PROXIES.remove(proxy)
            print(f"Removed failed proxy: {proxy}. {len(PROXIES)} proxies remaining.")
        return False
        
    except OSError as e:
        print(f"File system error for {path}: {e}")
        return False
        
    except Exception as e:
        print(f"Unexpected error for {url}: {e}")
        return False

def fetch_identifiers(identifier, details_dir, download_dir):
    """Fetch both details and download pages for one identifier."""
    details_url = f"{baseurl}/details/{identifier}"
    download_url = f"{baseurl}/download/{identifier}"

    # Add .html extension to create file paths
    details_path = os.path.join(details_dir, identifier + ".html")
    download_path = os.path.join(download_dir, identifier + ".html")

    # ALWAYS return both URLs - don't check if files exist
    # The processed_identifiers.txt is the single source of truth
    return [
        (details_url, details_path),
        (download_url, download_path)
    ]

def worker(task):
    """Worker for ThreadPoolExecutor."""
    url, path = task
    return save_pageurl(url, path)

if __name__ == "__main__":
    details_dir = "raw_html/details"
    download_dir = "raw_html/download"
    os.makedirs(details_dir, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)

    # Load identifiers
    with open("identifiers.txt") as f:
        identifiers = [line.strip() for line in f if line.strip()]
    
    print(f"Total identifiers found: {len(identifiers):,}")
    print(f"Total pages to potentially fetch: {len(identifiers) * 2:,}")
    
    if USE_PROXIES:
        print(f"Using {len(PROXIES)} proxies for rotation")
    else:
        print("No proxies configured - using direct connections")

    # Resume functionality - processed_identifiers.txt is the SINGLE source of truth
    processed_file = "processed_identifiers.txt"
    processed_identifiers = set()

    # Load already processed identifiers if resuming
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed_identifiers = set(line.strip() for line in f if line.strip())
        print(f"Resuming from previous run. Already processed: {len(processed_identifiers):,} identifiers")

    # Filter out already processed identifiers
    identifiers_to_process = [id for id in identifiers if id not in processed_identifiers]
    
    print(f"Identifiers remaining to process: {len(identifiers_to_process):,}")

    # If no identifiers to process, exit
    if len(identifiers_to_process) == 0:
        print("All identifiers have already been processed. Exiting.")
        exit()

    # Batch processing settings
    batch_size = 5000  # Process 5,000 identifiers per batch
    max_workers = 40   # Reduced workers to be more gentle with proxies
    total_batches = (len(identifiers_to_process) + batch_size - 1) // batch_size

    print(f"Processing in {total_batches} batches of {batch_size} identifiers each")

    # Track batch progress to handle interruptions
    batch_progress_file = "batch_progress.txt"
    current_batch = 0
    current_batch_start_idx = 0
    
    # Load batch progress if resuming
    if os.path.exists(batch_progress_file):
        with open(batch_progress_file, 'r') as f:
            progress_data = f.read().strip().split(',')
            if len(progress_data) == 2:
                current_batch = int(progress_data[0])
                current_batch_start_idx = int(progress_data[1])
                print(f"Resuming from batch {current_batch} at identifier index {current_batch_start_idx}")

    for batch_num in range(current_batch, total_batches):
        start_idx = batch_num * batch_size + current_batch_start_idx
        end_idx = min((batch_num + 1) * batch_size, len(identifiers_to_process))
        
        # Reset start index for subsequent batches
        if batch_num > current_batch:
            start_idx = batch_num * batch_size
            current_batch_start_idx = 0
        
        batch_identifiers = identifiers_to_process[start_idx:end_idx]
        
        print(f"\n{'='*60}")
        print(f"Processing batch {batch_num + 1}/{total_batches}")
        print(f"Identifiers: {start_idx + 1}-{end_idx} of {len(identifiers_to_process)}")
        if USE_PROXIES:
            print(f"Available proxies: {len(PROXIES)}")
        print(f"{'='*60}")

        # Build tasks for this batch - ALWAYS create tasks for all identifiers in batch
        all_tasks = []
        for identifier in batch_identifiers:
            all_tasks.extend(fetch_identifiers(identifier, details_dir, download_dir))
        
        print(f"Pages to fetch in this batch: {len(all_tasks):,}")

        # Process download tasks
        batch_start_time = time.time()
        successful_downloads = 0
        failed_downloads = 0
        newly_processed_identifiers = set()
        
        # Save batch progress before starting
        with open(batch_progress_file, 'w') as f:
            f.write(f"{batch_num},{start_idx - batch_num * batch_size}")
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(worker, task): task for task in all_tasks}
                
                for idx, future in enumerate(as_completed(futures)):
                    task = futures[future]
                    try:
                        result = future.result()
                        if result:
                            successful_downloads += 1
                            # Extract identifier from URL and mark for processing
                            url, path = task
                            identifier = url.split("/")[-1]
                            newly_processed_identifiers.add(identifier)
                        else:
                            failed_downloads += 1
                            
                    except Exception as e:
                        print(f"Task {task} failed with unexpected error: {e}")
                        failed_downloads += 1
                    
                    # Progress reporting
                    if (idx + 1) % 100 == 0:
                        elapsed = time.time() - batch_start_time
                        pages_per_sec = (idx + 1) / elapsed if elapsed > 0 else 0
                        print(f"Batch {batch_num + 1}: {idx + 1:,}/{len(all_tasks):,} "
                              f"({(idx + 1)/len(all_tasks)*100:.1f}%) "
                              f"- {pages_per_sec:.1f} pages/sec")
                        
                        if USE_PROXIES:
                            print(f"Available proxies: {len(PROXIES)}")
                    
                    # Update batch progress periodically
                    if (idx + 1) % 500 == 0:
                        with open(batch_progress_file, 'w') as f:
                            f.write(f"{batch_num},{start_idx - batch_num * batch_size + ((idx + 1) // 2)}")
            
            # Only mark identifiers as processed if BOTH their files were downloaded successfully
            successfully_processed_in_batch = set()
            for identifier in batch_identifiers:
            # Count how many successful downloads were for this identifier
                identifier_success_count = sum(1 for ident in newly_processed_identifiers if ident == identifier)
    
            # Each identifier should have exactly 2 files (details + download)
                if identifier_success_count == 2:
                    successfully_processed_in_batch.add(identifier)
                else:
                    print(f"Identifier {identifier} partially processed: {identifier_success_count}/2 files")
                    
            ## Update processed identifiers
            processed_identifiers.update(successfully_processed_in_batch)
            
            # Save progress after each batch
            with open(processed_file, 'w') as f:
                f.write("\n".join(sorted(processed_identifiers)))
            
            # Clear batch progress after successful completion
            if os.path.exists(batch_progress_file):
                os.remove(batch_progress_file)
            
            batch_time = time.time() - batch_start_time
            print(f"\nBatch {batch_num + 1} completed in {batch_time:.1f} seconds")
            print(f"Successful downloads: {successful_downloads:,}, Failed: {failed_downloads:,}")
            print(f"Fully processed identifiers: {len(successfully_processed_in_batch):,}")
            print(f"Partially processed identifiers: {len(batch_identifiers) - len(successfully_processed_in_batch):,}")
            
        except KeyboardInterrupt:
            print(f"\nBatch {batch_num + 1} interrupted. Progress saved.")
            print(f"Completed {successful_downloads + failed_downloads}/{len(all_tasks)} tasks in this batch.")
            break
        except Exception as e:
            print(f"Error processing batch {batch_num + 1}: {e}")
            # Save progress even on error
            with open(processed_file, 'w') as f:
                f.write("\n".join(sorted(processed_identifiers)))
        
        # Reset batch start index for next batch
        current_batch_start_idx = 0
        
        # Brief pause between batches to be server-friendly
        if batch_num < total_batches - 1:  # Don't pause after last batch
            pause_time = 15
            print(f"Pausing for {pause_time} seconds before next batch...")
            time.sleep(pause_time)

    # Clean up batch progress file if it exists
    if os.path.exists(batch_progress_file):
        os.remove(batch_progress_file)

    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE!")
    print(f"Total identifiers processed: {len(processed_identifiers):,}")
    print(f"Total pages downloaded: {len(processed_identifiers) * 2:,}")
    print(f"{'='*60}")