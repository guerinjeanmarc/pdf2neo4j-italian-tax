#!/usr/bin/env python3
"""
Script to download PDF files from URLs in the CSV file.
"""

import pandas as pd
import requests
from pathlib import Path
from urllib.parse import urlparse
import time
import re

def clean_filename(url):
    """Extract a clean filename from the URL."""
    parsed = urlparse(url)
    path = parsed.path
    
    # Get the last part of the path
    filename = path.split('/')[-1]
    
    # If no filename or doesn't end with .pdf, create one from the path
    if not filename or not filename.endswith('.pdf'):
        # Extract meaningful parts from the path
        parts = [p for p in path.split('/') if p and p.endswith('.pdf')]
        if parts:
            filename = parts[0]
        else:
            # Use a hash of the URL as fallback
            filename = f"document_{abs(hash(url))}.pdf"
    
    # Clean the filename
    filename = re.sub(r'[^\w\s\-\.]', '_', filename)
    return filename

def download_pdf(url, output_dir, timeout=30):
    """Download a PDF file from the given URL."""
    try:
        # Clean and prepare the URL
        url = url.strip()
        if not url or url.lower() == 'n/a':
            return None
        
        print(f"Downloading: {url}")
        
        # Make the request with headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        # Check if the content is actually a PDF
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type and not url.endswith('.pdf'):
            print(f"  Warning: Content type is {content_type}, might not be a PDF")
        
        # Generate filename
        filename = clean_filename(url)
        filepath = output_dir / filename
        
        # If file exists, add a number
        counter = 1
        base_filepath = filepath
        while filepath.exists():
            stem = base_filepath.stem
            suffix = base_filepath.suffix
            filepath = output_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        
        # Save the file
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"  ✓ Saved: {filepath.name}")
        return filepath
        
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error downloading {url}: {e}")
        return None
    except Exception as e:
        print(f"  ✗ Unexpected error for {url}: {e}")
        return None

def main():
    # Configuration
    csv_file = Path("data/sample_questions.csv")
    output_dir = Path("data")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True)
    
    print(f"Reading CSV file: {csv_file}")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Check if the column exists
    if 'LINK DOCUMENTI ESTRATTI' not in df.columns:
        print("Error: Column 'LINK DOCUMENTI ESTRATTI' not found in CSV")
        print(f"Available columns: {df.columns.tolist()}")
        return
    
    # Statistics
    total_downloads = 0
    successful_downloads = 0
    failed_downloads = 0
    
    # Process each row
    print(f"\nProcessing {len(df)} rows...")
    print("=" * 80)
    
    for idx, row in df.iterrows():
        links = row['LINK DOCUMENTI ESTRATTI']
        
        # Skip if no links or N/A
        if pd.isna(links) or str(links).strip().upper() == 'N/A' or str(links).strip() == '':
            continue
        
        # Split by newlines AND spaces to handle multiple URLs
        # Replace newlines with spaces, then split
        urls_raw = str(links).replace('\n', ' ').replace('\r', ' ').split()
        
        # Filter out empty strings and non-URL entries
        urls = [url for url in urls_raw if url and url.startswith('http')]
        
        if not urls:
            continue
        
        print(f"\nRow {idx + 1} - Question ID: {row.get('ID', 'N/A')}")
        
        for url in urls:
            url = url.strip()
            if url and url.lower() != 'n/a' and url.startswith('http'):
                total_downloads += 1
                result = download_pdf(url, output_dir)
                
                if result:
                    successful_downloads += 1
                else:
                    failed_downloads += 1
                
                # Be nice to the server
                time.sleep(0.5)
    
    # Print summary
    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"Total URLs processed: {total_downloads}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {failed_downloads}")
    print(f"\nFiles saved in: {output_dir.absolute()}")

if __name__ == "__main__":
    main()

