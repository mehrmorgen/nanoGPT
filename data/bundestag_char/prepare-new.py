import requests
from bs4 import BeautifulSoup
import os
import urllib.request
import re
from tqdm import tqdm
import time

# Base URL for the Bundestag Open Data page
BASE_URL = "https://www.bundestag.de"

# Directory to save downloaded files
DOWNLOAD_DIR = "bundestag_plenarprotokolle"

def create_directory(path):
    """Create a directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)

def download_file(url, filepath):
    """Download a file from a URL to the specified filepath with a progress bar."""
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    # Initialize progress bar
    with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(filepath)) as pbar:
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

def get_wahlperiode_from_context(link):
    """Extract Wahlperiode from the link's context or URL."""
    # Try to find the nearest parent with Wahlperiode information
    parent = link.find_parent('div', class_='bt-collapse')
    if parent:
        title_elem = parent.find('h2', class_='bt-collapse-title')
        if title_elem:
            wahlperiode_match = re.search(r'(\d+\.\s*Wahlperiode|1\.\s*-\s*19\.\s*Wahlperiode)', title_elem.text)
            if wahlperiode_match:
                return wahlperiode_match.group(0).replace(" ", "_").replace(".", "")
    
    # Fallback: Extract from URL if possible
    href = link['href']
    wahlperiode_match = re.search(r'wp(\d+)', href) or re.search(r'wahlperiode-(\d+)', href)
    if wahlperiode_match:
        return f"{wahlperiode_match.group(1)}_Wahlperiode"
    
    # Default to a generic directory if Wahlperiode can't be determined
    return "Unknown_Wahlperiode"

def scrape_plenarprotokolle():
    """Scrape and download all Plenarprotokolle with class='bt-link-dokument'."""
    # Create main download directory
    create_directory(DOWNLOAD_DIR)
    
    # Fetch the Open Data page
    response = requests.get(f"{BASE_URL}/services/opendata")
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all links with class="bt-link-dokument"
    links = soup.find_all('a', class_='bt-link-dokument', href=True)
    
    for link in links:
        href = link['href']
        if href.endswith('.xml') or href.endswith('.zip'):
            # Determine Wahlperiode
            wahlperiode = get_wahlperiode_from_context(link)
            wahlperiode_dir = os.path.join(DOWNLOAD_DIR, wahlperiode)
            create_directory(wahlperiode_dir)
            
            # Construct full URL
            file_url = href if href.startswith('http') else f"{BASE_URL}{href}"
            file_name = os.path.basename(href)
            file_path = os.path.join(wahlperiode_dir, file_name)
            
            # Skip if file already exists
            if os.path.exists(file_path):
                print(f"Skipping {file_name}, already downloaded.")
                continue
            
            # Download the file
            print(f"Downloading {file_name} to {wahlperiode_dir}...")
            try:
                download_file(file_url, file_path)
                time.sleep(1)  # Be polite to the server
            except Exception as e:
                print(f"Failed to download {file_name}: {e}")

if __name__ == "__main__":
    try:
        scrape_plenarprotokolle()
        print("Download completed successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")