#!/usr/bin/env python3
"""
Web Crawler for CyberDefender.hk
Downloads all pages from https://cyberdefender.hk/en-us/ and saves them locally
"""

import requests
from bs4 import BeautifulSoup
import os
import time
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path
import re
from datetime import datetime
import csv
import json

class CyberDefenderCrawler:
    def __init__(self, base_url="https://cyberdefender.hk/en-us/", output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\Anti-Scamming\\cytberdefender"):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.visited_urls = set()
        self.downloaded_files = []
        self.failed_urls = []
        self.site_map = []
        self.all_discovered_urls = set()
        self.unique_urls_to_crawl = set()
        self.url_normalization_cache = {}
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / "crawler.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def normalize_url(self, url):
        """Normalize URL to remove duplicates and variations"""
        parsed = urlparse(url)
        
        # Remove common query parameters that don't affect content
        query_params_to_remove = ['utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'gclid']
        if parsed.query:
            from urllib.parse import parse_qs, urlencode
            params = parse_qs(parsed.query)
            # Remove tracking parameters
            for param in query_params_to_remove:
                params.pop(param, None)
            # Rebuild query string
            new_query = urlencode(params, doseq=True)
            url = url.replace(parsed.query, new_query)
        
        # Remove trailing slash for consistency
        if url.endswith('/') and len(url) > 1:
            url = url[:-1]
            
        # Remove fragment
        if '#' in url:
            url = url.split('#')[0]
            
        return url
    
    def sanitize_filename(self, url):
        """Convert URL to safe filename"""
        # Remove protocol and domain
        path = urlparse(url).path
        if not path or path == '/':
            return 'index.html'
        
        # Remove leading slash and replace slashes with underscores
        filename = path.lstrip('/').replace('/', '_')
        
        # Handle query parameters in filename
        if '?' in filename:
            filename = filename.split('?')[0]
        
        # Add .html extension if not present
        if not filename.endswith(('.html', '.htm')):
            filename += '.html'
            
        return filename
    
    def download_page(self, url):
        """Download a single page"""
        try:
            self.logger.info(f"Downloading: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract all links from the page
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if self.is_valid_url(full_url):
                    # Normalize URL to remove duplicates
                    normalized_url = self.normalize_url(full_url)
                    links.append(normalized_url)
                    self.all_discovered_urls.add(normalized_url)
                    self.unique_urls_to_crawl.add(normalized_url)
            
            # Save the page
            filename = self.sanitize_filename(url)
            filepath = self.output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Add to site map
            self.site_map.append({
                'url': url,
                'filename': filename,
                'title': soup.title.string if soup.title else 'No Title',
                'links_found': len(links),
                'timestamp': datetime.now().isoformat(),
                'status': 'success'
            })
            
            self.downloaded_files.append({
                'url': url,
                'filename': filename,
                'timestamp': datetime.now().isoformat(),
                'links_found': len(links)
            })
            
            self.logger.info(f"Saved: {filename} ({len(links)} links found)")
            return links
            
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            self.failed_urls.append({'url': url, 'error': str(e)})
            
            # Add failed URL to site map
            self.site_map.append({
                'url': url,
                'filename': 'FAILED',
                'title': 'Failed Download',
                'links_found': 0,
                'timestamp': datetime.now().isoformat(),
                'status': 'failed',
                'error': str(e)
            })
            return []
    
    def is_valid_url(self, url):
        """Check if URL should be crawled"""
        parsed = urlparse(url)
        
        # Only crawl URLs from the same domain
        if parsed.netloc != self.domain:
            return False
            
        # Skip certain file types
        skip_extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.ico']
        if any(url.lower().endswith(ext) for ext in skip_extensions):
            return False
            
        # Skip certain URL patterns
        skip_patterns = ['#', 'javascript:', 'mailto:', 'tel:']
        if any(pattern in url.lower() for pattern in skip_patterns):
            return False
            
        return True
    
    def crawl(self, max_pages=100, delay=1):
        """Main crawling function with duplicate handling"""
        self.logger.info(f"Starting crawl of {self.base_url}")
        self.logger.info(f"Output directory: {self.output_dir}")
        
        # Start with base URL
        urls_to_visit = [self.normalize_url(self.base_url)]
        pages_crawled = 0
        
        while urls_to_visit and pages_crawled < max_pages:
            current_url = urls_to_visit.pop(0)
            
            if current_url in self.visited_urls:
                continue
                
            self.visited_urls.add(current_url)
            
            # Download the page
            new_links = self.download_page(current_url)
            pages_crawled += 1
            
            # Add new unique links to the queue
            for link in new_links:
                if (link not in self.visited_urls and 
                    link not in urls_to_visit and 
                    link not in self.unique_urls_to_crawl):
                    urls_to_visit.append(link)
                    self.unique_urls_to_crawl.add(link)
            
            # Respectful crawling delay
            time.sleep(delay)
            
            self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(urls_to_visit)} URLs in queue, {len(self.unique_urls_to_crawl)} unique URLs discovered")
        
        self.generate_summary()
        self.generate_sitemap_csv()
        self.logger.info("Crawling completed!")
    
    def crawl_all_discovered(self, max_pages=500, delay=1):
        """Crawl all discovered unique URLs"""
        self.logger.info(f"Starting comprehensive crawl of all discovered URLs")
        self.logger.info(f"Total unique URLs to crawl: {len(self.unique_urls_to_crawl)}")
        
        # Convert set to list for processing
        urls_to_visit = list(self.unique_urls_to_crawl)
        pages_crawled = 0
        
        for url in urls_to_visit:
            if pages_crawled >= max_pages:
                break
                
            if url in self.visited_urls:
                continue
                
            self.visited_urls.add(url)
            
            # Download the page
            new_links = self.download_page(url)
            pages_crawled += 1
            
            # Respectful crawling delay
            time.sleep(delay)
            
            if pages_crawled % 50 == 0:
                self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(self.unique_urls_to_crawl)} total unique URLs discovered")
        
        self.generate_summary()
        self.generate_sitemap_csv()
        self.logger.info("Comprehensive crawling completed!")
    
    def generate_summary(self):
        """Generate a summary of the crawling results"""
        summary_file = self.output_dir / "crawl_summary.md"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("# CyberDefender Crawl Summary\n\n")
            f.write(f"**Crawl Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Base URL:** {self.base_url}\n\n")
            f.write(f"**Total Pages Downloaded:** {len(self.downloaded_files)}\n\n")
            f.write(f"**Failed Downloads:** {len(self.failed_urls)}\n\n")
            
            f.write("## Downloaded Files\n\n")
            for file_info in self.downloaded_files:
                f.write(f"- **{file_info['filename']}** - {file_info['url']} ({file_info['links_found']} links)\n")
            
            if self.failed_urls:
                f.write("\n## Failed Downloads\n\n")
                for failed in self.failed_urls:
                    f.write(f"- **{failed['url']}** - {failed['error']}\n")
        
        self.logger.info(f"Summary saved to: {summary_file}")
    
    def generate_sitemap_csv(self):
        """Generate a CSV site map of all discovered pages"""
        csv_file = self.output_dir / "sitemap.csv"
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'URL', 'Filename', 'Title', 'Links Found', 'Status', 
                'Timestamp', 'Error (if failed)', 'Category'
            ])
            
            # Write site map data
            for page in self.site_map:
                # Determine category based on URL
                category = self.categorize_url(page['url'])
                
                writer.writerow([
                    page['url'],
                    page['filename'],
                    page['title'],
                    page['links_found'],
                    page['status'],
                    page['timestamp'],
                    page.get('error', ''),
                    category
                ])
        
        # Also create a comprehensive URL list
        url_list_file = self.output_dir / "all_discovered_urls.txt"
        with open(url_list_file, 'w', encoding='utf-8') as f:
            for url in sorted(self.all_discovered_urls):
                f.write(f"{url}\n")
        
        self.logger.info(f"Site map CSV saved to: {csv_file}")
        self.logger.info(f"All discovered URLs saved to: {url_list_file}")
        self.logger.info(f"Total unique URLs discovered: {len(self.all_discovered_urls)}")
    
    def categorize_url(self, url):
        """Categorize URL based on path"""
        if '/it-basics' in url:
            return 'IT Basics'
        elif '/secure-your-device' in url:
            return 'Secure Your Device'
        elif '/parents-and-teachers' in url:
            return 'Parents & Teachers'
        elif '/cybercrime' in url:
            return 'Cybercrime'
        elif '/events' in url or '/resources' in url:
            return 'Resources & Events'
        elif '/scameter' in url:
            return 'Scameter'
        elif url.endswith('/') or url == self.base_url:
            return 'Main Pages'
        else:
            return 'Other'

def main():
    """Main function to run the crawler"""
    crawler = CyberDefenderCrawler()
    crawler.crawl(max_pages=200, delay=1)

if __name__ == "__main__":
    main()
