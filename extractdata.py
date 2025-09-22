import os
import json
import csv
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_origpath_from_section2(html_content):
    """Extract dc.identifier.origpath from section 2 HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for the origpath pattern in the HTML
    origpath_pattern = re.compile(r'dc\.identifier\.origpath:', re.IGNORECASE)
    
    # Find all text that contains the pattern
    for element in soup.find_all(string=origpath_pattern):
        text = element.strip()
        if 'dc.identifier.origpath:' in text.lower():
            # Extract the path part
            match = re.search(r'dc\.identifier\.origpath:\s*(/.+)', text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    
    return None

def extract_details_sections(html_content):
    """Extract sections 1, 2, 3 from details page HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    sections = {}
    
    # Section 1: div class="row metadata-list" role="list"
    section1 = soup.find('div', class_='row metadata-list', role='list')
    if section1:
        sections['section1'] = str(section1)
    
    # Section 2: div id="descript" itemprop="description"
    section2 = soup.find('div', id='descript', itemprop='description')
    if section2:
        sections['section2'] = str(section2)
    
    # Section 3: div class="metadata-expandable-list row" role="list"
    section3 = soup.find('div', class_='metadata-expandable-list row', role='list')
    if section3:
        sections['section3'] = str(section3)
    
    return sections

def extract_download_section(html_content):
    """Extract section 4 (file table HTML) from download page"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Section 4: table class="directory-listing-table"
    section4 = soup.find('table', class_='directory-listing-table')
    if section4:
        return str(section4)
    return ''

def process_single_identifier(identifier, details_dir, download_dir):
    """Process one identifier - this function will run in parallel"""
    details_file = os.path.join(details_dir, f"{identifier}.html")
    download_file = os.path.join(download_dir, f"{identifier}.html")
    
    if not os.path.exists(details_file):
        print(f"Missing details file for: {identifier}")
        return None
    
    if not os.path.exists(download_file):
        print(f"Missing download file for: {identifier}")
        return None
    
    try:
        # Extract from details page
        with open(details_file, 'r', encoding='utf-8') as f:
            details_content = f.read()
        
        details_sections = extract_details_sections(details_content)
        
        # Extract origpath from section 2
        origpath = None
        if details_sections.get('section2'):
            origpath = extract_origpath_from_section2(details_sections['section2'])
        
        # Extract from download page
        with open(download_file, 'r', encoding='utf-8') as f:
            download_content = f.read()
        
        section4_html = extract_download_section(download_content)
        
        # Build structured data with origpath
        return {
            "identifier": identifier,
            "dc.identifier.origpath": origpath,
            "section1": details_sections.get('section1', ''),
            "section2": details_sections.get('section2', ''),
            "section3": details_sections.get('section3', ''),
            "section4": section4_html
        }
        
    except Exception as e:
        print(f"Error processing {identifier}: {e}")
        return None

def process_identifiers_parallel(identifiers, details_dir, download_dir, max_workers=10):
    """Process identifiers in parallel using ThreadPoolExecutor"""
    all_data = []
    processed_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_identifier = {
            executor.submit(process_single_identifier, identifier, details_dir, download_dir): identifier 
            for identifier in identifiers
        }
        
        # Process completed tasks
        for future in as_completed(future_to_identifier):
            identifier = future_to_identifier[future]
            try:
                data = future.result()
                if data:
                    all_data.append(data)
                    processed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"Task for {identifier} generated exception: {e}")
                failed_count += 1
            
            # Progress reporting
            if (processed_count + failed_count) % 100 == 0:
                print(f"Progress: {processed_count + failed_count}/{len(identifiers)} "
                      f"({processed_count} success, {failed_count} failed)")
    
    return all_data, processed_count, failed_count

def process_all_identifiers_parallel(identifiers_file, details_dir, download_dir, output_dir, max_workers=20):
    """Process all identifiers in parallel and save results with resume functionality"""
    # Read identifiers
    with open(identifiers_file, 'r', encoding='utf-8') as f:
        all_identifiers = [line.strip() for line in f if line.strip()]
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Resume functionality - track already processed identifiers
    processed_file = os.path.join(output_dir, 'processed_extracted_identifiers.txt')
    processed_identifiers = set()
    
    if os.path.exists(processed_file):
        with open(processed_file, 'r', encoding='utf-8') as f:
            processed_identifiers = set(line.strip() for line in f if line.strip())
        print(f"Resuming extraction. Already processed: {len(processed_identifiers):,} identifiers")
    
    # Filter out already processed identifiers
    identifiers_to_process = [id for id in all_identifiers if id not in processed_identifiers]
    
    print(f"Starting parallel processing of {len(identifiers_to_process)} identifiers with {max_workers} workers...")
    print(f"Total identifiers: {len(all_identifiers):,}, Already processed: {len(processed_identifiers):,}")
    
    if len(identifiers_to_process) == 0:
        print("All identifiers have already been extracted. Exiting.")
        return 0
    
    # Process in parallel
    all_data, processed_count, failed_count = process_identifiers_parallel(
        identifiers_to_process, details_dir, download_dir, max_workers
    )
    
    # Save combined JSON
    combined_file = os.path.join(output_dir, "all_data.json")
    
    # If combined file already exists, load and append new data
    if os.path.exists(combined_file):
        with open(combined_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_data.extend(all_data)
        all_data = existing_data
    
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"Saved combined file: {combined_file}")
    
    # Save individual files in parallel too
    json_dir = os.path.join(output_dir, 'individual')
    os.makedirs(json_dir, exist_ok=True)
    
    print("Saving individual files...")
    save_individual_files_parallel(all_data, json_dir, max_workers=min(10, max_workers))
    
    # Update processed identifiers
    processed_identifiers.update(identifiers_to_process)
    with open(processed_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(sorted(processed_identifiers)))
    
    # Create summary
    summary = {
        'total_identifiers': len(all_identifiers),
        'successfully_processed': len(processed_identifiers),
        'failed_processing': failed_count,
        'processing_date': datetime.utcnow().isoformat() + 'Z',
        'output_files': {
            'combined': combined_file,
            'individual_directory': json_dir,
            'processed_tracker': processed_file
        },
        'parallel_workers': max_workers
    }
    
    with open(os.path.join(output_dir, 'processing_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\nParallel processing complete!")
    print(f"Success: {len(processed_identifiers):,}, Failed: {failed_count}")
    print(f"Individual files directory: {json_dir}")
    print(f"Processed tracker: {processed_file}")
    
    return len(processed_identifiers)

def save_individual_files_parallel(data_list, output_dir, max_workers=10):
    """Save individual JSON files in parallel"""
    os.makedirs(output_dir, exist_ok=True)
    
    def save_single_file(data):
        try:
            individual_file = os.path.join(output_dir, f"{data['identifier']}.json")
            with open(individual_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving {data['identifier']}: {e}")
            return False
    
    saved_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_data = {executor.submit(save_single_file, data): data for data in data_list}
        
        for future in as_completed(future_to_data):
            try:
                if future.result():
                    saved_count += 1
                if saved_count % 1000 == 0:
                    print(f"Saved {saved_count} individual files...")
            except Exception as e:
                print(f"Error in file saving: {e}")
    
    print(f"Saved {saved_count} individual files")
    return saved_count

# For very large datasets, process in batches with parallel processing
def process_in_batches_parallel(identifiers_file, details_dir, download_dir, output_dir, 
                               batch_size=50000, max_workers=20):
    """Process very large datasets in batches with parallel processing"""
    os.makedirs(output_dir, exist_ok=True)
    
    with open(identifiers_file, 'r', encoding='utf-8') as f:
        all_identifiers = [line.strip() for line in f if line.strip()]
    
    # Resume functionality
    processed_file = os.path.join(output_dir, 'processed_extracted_identifiers.txt')
    processed_identifiers = set()
    
    if os.path.exists(processed_file):
        with open(processed_file, 'r', encoding='utf-8') as f:
            processed_identifiers = set(line.strip() for line in f if line.strip())
    
    # Filter out already processed identifiers
    identifiers_to_process = [id for id in all_identifiers if id not in processed_identifiers]
    
    total_batches = (len(identifiers_to_process) + batch_size - 1) // batch_size
    total_processed = len(processed_identifiers)
    total_failed = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(identifiers_to_process))
        batch_identifiers = identifiers_to_process[start_idx:end_idx]
        
        print(f"\nProcessing batch {batch_num + 1}/{total_batches} ({start_idx}-{end_idx})")
        
        batch_data, processed, failed = process_identifiers_parallel(
            batch_identifiers, details_dir, download_dir, max_workers
        )
        
        # Save batch
        batch_file = os.path.join(output_dir, f"batch_{batch_num + 1}.json")
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved batch {batch_num + 1} with {len(batch_data)} records")
        
        # Update processed identifiers
        processed_identifiers.update(batch_identifiers)
        with open(processed_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(sorted(processed_identifiers)))
        
        total_processed += processed
        total_failed += failed
    
    return total_processed, total_failed

if __name__ == "__main__":
    import time
    
    # Configuration
    IDENTIFIERS_FILE = "identifiers.txt"
    DETAILS_DIR = "raw_html/details"
    DOWNLOAD_DIR = "raw_html/download"
    OUTPUT_DIR = "extracted_data"
    MAX_WORKERS = 40
    
    start_time = time.time()
    
    # Ensure all input directories exist
    if not os.path.exists(DETAILS_DIR):
        os.makedirs(DETAILS_DIR, exist_ok=True)
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    if os.path.exists(IDENTIFIERS_FILE):
        with open(IDENTIFIERS_FILE, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
        
        print(f"Found {line_count} identifiers")
        
        # For very large datasets, use batched parallel processing
        if line_count > 200000:
            print("Very large dataset detected. Using batched parallel processing...")
            processed, failed = process_in_batches_parallel(
                IDENTIFIERS_FILE, DETAILS_DIR, DOWNLOAD_DIR, OUTPUT_DIR,
                batch_size=50000, max_workers=MAX_WORKERS
            )
        else:
            # Use regular parallel processing
            processed = process_all_identifiers_parallel(
                IDENTIFIERS_FILE, DETAILS_DIR, DOWNLOAD_DIR, OUTPUT_DIR, 
                max_workers=MAX_WORKERS
            )
            failed = line_count - processed
    else:
        print("Identifiers file not found!")
        processed = failed = 0
    
    end_time = time.time()
    
    total_time = end_time - start_time
    print(f"\nTotal processing time: {total_time:.2f} seconds")
    if processed > 0:
        print(f"Time per identifier: {total_time/processed:.4f} seconds")
    print(f"Final result: {processed} successful, {failed} failed")