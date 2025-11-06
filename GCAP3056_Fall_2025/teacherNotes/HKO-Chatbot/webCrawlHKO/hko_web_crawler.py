#!/usr/bin/env python3
"""
Web Crawler for Hong Kong Observatory (HKO)
Downloads all pages from https://www.hko.gov.hk/en/index.html and searches for "Dr Tin chatbot" mentions
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

class HKOWebCrawler:
    def __init__(self, base_url="https://www.hko.gov.hk/en/index.html", 
                 output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO"):
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
        self.dr_tin_mentions = []  # Store pages that mention Dr Tin chatbot
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for organization
        self.pages_dir = self.output_dir / "downloaded_pages"
        self.dr_tin_pages_dir = self.output_dir / "dr_tin_mentions"
        self.reports_dir = self.output_dir / "reports"
        
        for dir_path in [self.pages_dir, self.dr_tin_pages_dir, self.reports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / "hko_crawler.log"
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
    
    def check_dr_tin_mention(self, content, url):
        """Check if content mentions Dr Tin chatbot"""
        # Search for various forms of Dr Tin chatbot mentions
        patterns = [
            r'dr\s+tin\s+chatbot',
            r'dr\s+tin\s+bot',
            r'dr\.\s*tin\s+chatbot',
            r'dr\.\s*tin\s+bot',
            r'chatbot.*dr\s+tin',
            r'bot.*dr\s+tin',
            r'dr\s+tin.*chatbot',
            r'dr\s+tin.*bot'
        ]
        
        content_lower = content.lower()
        mentions = []
        
        for pattern in patterns:
            matches = re.finditer(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                # Get context around the match
                start = max(0, match.start() - 100)
                end = min(len(content), match.end() + 100)
                context = content[start:end]
                mentions.append({
                    'pattern': pattern,
                    'match': match.group(),
                    'context': context.strip(),
                    'position': match.start()
                })
        
        if mentions:
            self.dr_tin_mentions.append({
                'url': url,
                'mentions': mentions,
                'timestamp': datetime.now().isoformat()
            })
            return True
        
        return False
    
    def download_page(self, url):
        """Download a single page"""
        try:
            self.logger.info(f"Downloading: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse content
            soup = BeautifulSoup(response.content, 'html.parser')
            content = soup.get_text()
            
            # Check for Dr Tin chatbot mentions
            has_dr_tin_mention = self.check_dr_tin_mention(content, url)
            
            # Save the page
            filename = self.sanitize_filename(url)
            file_path = self.pages_dir / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # If it mentions Dr Tin, also save to special directory
            if has_dr_tin_mention:
                dr_tin_file_path = self.dr_tin_pages_dir / filename
                with open(dr_tin_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                self.logger.info(f"Dr Tin mention found in: {url}")
            
            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if self.is_valid_url(full_url):
                    links.append(full_url)
            
            # Store page information
            page_info = {
                'url': url,
                'filename': filename,
                'status': 'success',
                'links_found': len(links),
                'has_dr_tin_mention': has_dr_tin_mention,
                'timestamp': datetime.now().isoformat(),
                'links': links
            }
            
            self.site_map.append(page_info)
            self.downloaded_files.append(str(file_path))
            
            return page_info
            
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            self.failed_urls.append(url)
            
            page_info = {
                'url': url,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            self.site_map.append(page_info)
            return None
    
    def is_valid_url(self, url):
        """Check if URL is valid for crawling"""
        try:
            parsed = urlparse(url)
            return (
                parsed.netloc == self.domain and
                not any(ext in url.lower() for ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.xml', '.zip', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']) and
                not any(path in url.lower() for path in ['/api/', '/ajax/', '/search?', '/download/'])
            )
        except:
            return False
    
    def crawl(self, max_pages=500, delay=1):
        """Main crawling function"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING HKO WEB CRAWL")
        self.logger.info("=" * 60)
        self.logger.info(f"Base URL: {self.base_url}")
        self.logger.info(f"Max pages: {max_pages}")
        self.logger.info(f"Output directory: {self.output_dir}")
        
        urls_to_visit = [self.base_url]
        pages_crawled = 0
        
        while urls_to_visit and pages_crawled < max_pages:
            current_url = urls_to_visit.pop(0)
            
            # Normalize URL
            normalized_url = self.normalize_url(current_url)
            
            if normalized_url in self.visited_urls:
                continue
                
            self.visited_urls.add(normalized_url)
            
            # Download page
            page_info = self.download_page(normalized_url)
            
            if page_info and page_info['status'] == 'success':
                # Add new links to queue
                for link in page_info['links']:
                    normalized_link = self.normalize_url(link)
                    if normalized_link not in self.visited_urls and normalized_link not in urls_to_visit:
                        urls_to_visit.append(normalized_link)
                        self.all_discovered_urls.add(normalized_link)
            
            pages_crawled += 1
            
            # Progress update
            if pages_crawled % 10 == 0:
                self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(self.dr_tin_mentions)} Dr Tin mentions found")
            
            # Delay between requests
            time.sleep(delay)
        
        # Generate reports
        self.generate_reports()
        
        self.logger.info("=" * 60)
        self.logger.info("HKO CRAWL COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total pages crawled: {pages_crawled}")
        self.logger.info(f"Dr Tin mentions found: {len(self.dr_tin_mentions)}")
        self.logger.info(f"Failed downloads: {len(self.failed_urls)}")
    
    def generate_reports(self):
        """Generate comprehensive reports"""
        # Generate CSV sitemap
        sitemap_file = self.reports_dir / "hko_sitemap.csv"
        with open(sitemap_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['URL', 'Filename', 'Status', 'Links Found', 'Has Dr Tin Mention', 'Timestamp'])
            for page in self.site_map:
                writer.writerow([
                    page['url'],
                    page.get('filename', ''),
                    page['status'],
                    page.get('links_found', 0),
                    page.get('has_dr_tin_mention', False),
                    page.get('timestamp', '')
                ])
        
        # Generate Dr Tin mentions report
        dr_tin_file = self.reports_dir / "dr_tin_mentions_report.md"
        with open(dr_tin_file, 'w', encoding='utf-8') as f:
            f.write("# Dr Tin Chatbot Mentions Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Total mentions found:** {len(self.dr_tin_mentions)}\n\n")
            
            for i, mention in enumerate(self.dr_tin_mentions, 1):
                f.write(f"## {i}. {mention['url']}\n\n")
                f.write(f"**Found at:** {mention['timestamp']}\n\n")
                f.write("**Mentions:**\n")
                for j, match in enumerate(mention['mentions'], 1):
                    f.write(f"{j}. **Pattern:** `{match['pattern']}`\n")
                    f.write(f"   **Match:** `{match['match']}`\n")
                    f.write(f"   **Context:** {match['context']}\n\n")
                f.write("---\n\n")
        
        # Generate comprehensive summary
        summary_file = self.reports_dir / "hko_crawl_summary.md"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Web Crawl Summary\n\n")
            f.write(f"**Crawl Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Base URL:** {self.base_url}\n\n")
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Total Pages Crawled:** {len(self.visited_urls)}\n")
            f.write(f"- **Successful Downloads:** {len([p for p in self.site_map if p['status'] == 'success'])}\n")
            f.write(f"- **Failed Downloads:** {len([p for p in self.site_map if p['status'] == 'failed'])}\n")
            f.write(f"- **Dr Tin Chatbot Mentions Found:** {len(self.dr_tin_mentions)}\n\n")
            
            f.write("## Files Generated\n\n")
            f.write("- `downloaded_pages/` - All downloaded HTML pages\n")
            f.write("- `dr_tin_mentions/` - Pages containing Dr Tin chatbot mentions\n")
            f.write("- `reports/hko_sitemap.csv` - Complete sitemap in CSV format\n")
            f.write("- `reports/dr_tin_mentions_report.md` - Detailed Dr Tin mentions report\n")
            f.write("- `reports/hko_crawl_summary.md` - This summary\n")
            f.write("- `hko_crawler.log` - Detailed logs\n")
        
        # Update the main log file
        self.update_main_log()
        
        self.logger.info(f"Reports generated in: {self.reports_dir}")
    
    def update_main_log(self):
        """Update the main log file with crawl results"""
        log_file = self.output_dir / "webCrawllog_HKO.md"
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Web Crawl Log\n\n")
            f.write(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Crawl Summary\n\n")
            f.write(f"- **Total Pages Crawled:** {len(self.visited_urls)}\n")
            f.write(f"- **Dr Tin Chatbot Mentions Found:** {len(self.dr_tin_mentions)}\n")
            f.write(f"- **Failed Downloads:** {len(self.failed_urls)}\n\n")
            
            if self.dr_tin_mentions:
                f.write("## Dr Tin Chatbot Mentions Found\n\n")
                for i, mention in enumerate(self.dr_tin_mentions, 1):
                    f.write(f"{i}. [{mention['url']}]({mention['url']})\n")
            else:
                f.write("## No Dr Tin Chatbot Mentions Found\n\n")
                f.write("The crawler did not find any pages mentioning 'Dr Tin chatbot' on the HKO website.\n")
            
            f.write(f"\n## Files Generated\n\n")
            f.write(f"- Downloaded pages: `downloaded_pages/`\n")
            f.write(f"- Dr Tin mentions: `dr_tin_mentions/`\n")
            f.write(f"- Reports: `reports/`\n")
            f.write(f"- Detailed logs: `hko_crawler.log`\n")

def main():
    """Main function to run the HKO crawler"""
    print("Starting HKO Web Crawler...")
    print("Searching for 'Dr Tin chatbot' mentions on HKO website")
    print("=" * 60)
    
    crawler = HKOWebCrawler()
    
    try:
        crawler.crawl(max_pages=500, delay=1)
        
        print("\n" + "=" * 60)
        print("HKO CRAWL COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Total pages crawled: {len(crawler.visited_urls)}")
        print(f"Dr Tin chatbot mentions found: {len(crawler.dr_tin_mentions)}")
        print(f"Files saved to: {crawler.output_dir}")
        print("\nGenerated files:")
        print(f"  - downloaded_pages/ (all HTML pages)")
        print(f"  - dr_tin_mentions/ (pages with Dr Tin mentions)")
        print(f"  - reports/ (CSV sitemap and detailed reports)")
        print(f"  - webCrawllog_HKO.md (main log)")
        print(f"  - hko_crawler.log (detailed logs)")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
        print("Partial results have been saved.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


