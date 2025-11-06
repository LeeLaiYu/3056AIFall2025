#!/usr/bin/env python3
"""
Enhanced HKO Web Crawler with Advanced Content Analysis
Downloads all pages from HKO website and performs detailed analysis for Dr Tin chatbot mentions
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
from content_analyzer import HKOContentAnalyzer

class EnhancedHKOWebCrawler:
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
        self.dr_tin_mentions = []
        
        # Initialize content analyzer
        self.content_analyzer = HKOContentAnalyzer(output_dir)
        
        # Create output directory structure
        self.setup_directories()
        
        # Setup logging
        self.setup_logging()
        
    def setup_directories(self):
        """Create organized directory structure"""
        directories = [
            self.output_dir,
            self.output_dir / "downloaded_pages",
            self.output_dir / "dr_tin_mentions", 
            self.output_dir / "high_relevance_pages",
            self.output_dir / "reports",
            self.output_dir / "logs"
        ]
        
        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_file = self.output_dir / "logs" / "enhanced_hko_crawler.log"
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
            for param in query_params_to_remove:
                params.pop(param, None)
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
        path = urlparse(url).path
        if not path or path == '/':
            return 'index.html'
        
        filename = path.lstrip('/').replace('/', '_')
        
        if '?' in filename:
            filename = filename.split('?')[0]
        
        if not filename.endswith(('.html', '.htm')):
            filename += '.html'
            
        return filename
    
    def is_valid_url(self, url):
        """Check if URL is valid for crawling"""
        try:
            parsed = urlparse(url)
            return (
                parsed.netloc == self.domain and
                not any(ext in url.lower() for ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.xml', '.zip', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']) and
                not any(path in url.lower() for path in ['/api/', '/ajax/', '/search?', '/download/']) and
                'javascript:' not in url.lower()
            )
        except:
            return False
    
    def download_and_analyze_page(self, url):
        """Download page and perform content analysis"""
        try:
            self.logger.info(f"Downloading and analyzing: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            content = soup.get_text()
            
            # Save the page
            filename = self.sanitize_filename(url)
            file_path = self.output_dir / "downloaded_pages" / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Perform content analysis
            analysis = self.content_analyzer.analyze_content(content, url, filename)
            
            # Save to appropriate directory based on analysis
            if analysis['has_dr_tin_mention']:
                dr_tin_file_path = self.output_dir / "dr_tin_mentions" / filename
                with open(dr_tin_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                self.logger.info(f"Dr Tin mention found in: {url}")
            
            if analysis['relevance_score'] > 0.5:
                high_relevance_file_path = self.output_dir / "high_relevance_pages" / filename
                with open(high_relevance_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                self.logger.info(f"High relevance content found in: {url}")
            
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
                'has_dr_tin_mention': analysis['has_dr_tin_mention'],
                'relevance_score': analysis['relevance_score'],
                'dr_tin_mentions_count': len(analysis['dr_tin_mentions']),
                'related_keywords_count': len(analysis['related_keywords']),
                'timestamp': datetime.now().isoformat(),
                'links': links,
                'analysis': analysis
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
    
    def crawl(self, max_pages=500, delay=1):
        """Main crawling function with enhanced analysis"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING ENHANCED HKO WEB CRAWL")
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
            
            # Download and analyze page
            page_info = self.download_and_analyze_page(normalized_url)
            
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
                dr_tin_count = len([p for p in self.site_map if p.get('has_dr_tin_mention', False)])
                high_relevance_count = len([p for p in self.site_map if p.get('relevance_score', 0) > 0.5])
                self.logger.info(f"Progress: {pages_crawled} pages crawled, {dr_tin_count} Dr Tin mentions, {high_relevance_count} high relevance")
            
            # Delay between requests
            time.sleep(delay)
        
        # Generate comprehensive reports
        self.generate_comprehensive_reports()
        
        self.logger.info("=" * 60)
        self.logger.info("ENHANCED HKO CRAWL COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total pages crawled: {pages_crawled}")
        self.logger.info(f"Dr Tin mentions found: {len([p for p in self.site_map if p.get('has_dr_tin_mention', False)])}")
        self.logger.info(f"High relevance pages: {len([p for p in self.site_map if p.get('relevance_score', 0) > 0.5])}")
        self.logger.info(f"Failed downloads: {len(self.failed_urls)}")
    
    def generate_comprehensive_reports(self):
        """Generate comprehensive reports with analysis"""
        # Generate CSV sitemap with analysis data
        sitemap_file = self.output_dir / "reports" / "enhanced_hko_sitemap.csv"
        with open(sitemap_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'URL', 'Filename', 'Status', 'Links Found', 'Has Dr Tin Mention', 
                'Relevance Score', 'Dr Tin Mentions Count', 'Related Keywords Count', 'Timestamp'
            ])
            for page in self.site_map:
                writer.writerow([
                    page['url'],
                    page.get('filename', ''),
                    page['status'],
                    page.get('links_found', 0),
                    page.get('has_dr_tin_mention', False),
                    page.get('relevance_score', 0),
                    page.get('dr_tin_mentions_count', 0),
                    page.get('related_keywords_count', 0),
                    page.get('timestamp', '')
                ])
        
        # Generate Dr Tin mentions report
        dr_tin_pages = [p for p in self.site_map if p.get('has_dr_tin_mention', False)]
        dr_tin_file = self.output_dir / "reports" / "dr_tin_mentions_detailed.md"
        with open(dr_tin_file, 'w', encoding='utf-8') as f:
            f.write("# Dr Tin Chatbot Mentions - Detailed Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Total pages with Dr Tin mentions:** {len(dr_tin_pages)}\n\n")
            
            for i, page in enumerate(dr_tin_pages, 1):
                f.write(f"## {i}. [{page['url']}]({page['url']})\n\n")
                f.write(f"**Filename:** {page.get('filename', 'N/A')}\n")
                f.write(f"**Relevance Score:** {page.get('relevance_score', 0):.2f}\n")
                f.write(f"**Dr Tin Mentions Count:** {page.get('dr_tin_mentions_count', 0)}\n")
                f.write(f"**Related Keywords Count:** {page.get('related_keywords_count', 0)}\n\n")
                
                if 'analysis' in page and 'dr_tin_mentions' in page['analysis']:
                    f.write("**Detailed Mentions:**\n")
                    for j, mention in enumerate(page['analysis']['dr_tin_mentions'], 1):
                        f.write(f"{j}. **Pattern:** `{mention['pattern']}`\n")
                        f.write(f"   **Match:** `{mention['match']}`\n")
                        f.write(f"   **Confidence:** {mention['confidence']:.2f}\n")
                        f.write(f"   **Context:** {mention['context']}\n\n")
                
                f.write("---\n\n")
        
        # Generate comprehensive summary
        summary_file = self.output_dir / "reports" / "enhanced_crawl_summary.md"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("# Enhanced HKO Web Crawl Summary\n\n")
            f.write(f"**Crawl Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Base URL:** {self.base_url}\n\n")
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Total Pages Crawled:** {len(self.visited_urls)}\n")
            f.write(f"- **Successful Downloads:** {len([p for p in self.site_map if p['status'] == 'success'])}\n")
            f.write(f"- **Failed Downloads:** {len([p for p in self.site_map if p['status'] == 'failed'])}\n")
            f.write(f"- **Dr Tin Chatbot Mentions Found:** {len(dr_tin_pages)}\n")
            
            high_relevance_pages = [p for p in self.site_map if p.get('relevance_score', 0) > 0.5]
            f.write(f"- **High Relevance Pages:** {len(high_relevance_pages)}\n\n")
            
            f.write("## Content Analysis Results\n\n")
            if dr_tin_pages:
                f.write("### Pages with Dr Tin Mentions\n\n")
                for page in dr_tin_pages:
                    f.write(f"- [{page['url']}]({page['url']}) (Score: {page.get('relevance_score', 0):.2f})\n")
            else:
                f.write("### No Direct Dr Tin Mentions Found\n\n")
                f.write("The crawler did not find any pages with direct mentions of 'Dr Tin chatbot'.\n\n")
            
            if high_relevance_pages:
                f.write("### High Relevance Pages\n\n")
                for page in high_relevance_pages:
                    f.write(f"- [{page['url']}]({page['url']}) (Score: {page.get('relevance_score', 0):.2f})\n")
            
            f.write("\n## Files Generated\n\n")
            f.write("- `downloaded_pages/` - All downloaded HTML pages\n")
            f.write("- `dr_tin_mentions/` - Pages containing Dr Tin chatbot mentions\n")
            f.write("- `high_relevance_pages/` - Pages with high relevance scores\n")
            f.write("- `reports/enhanced_hko_sitemap.csv` - Complete sitemap with analysis\n")
            f.write("- `reports/dr_tin_mentions_detailed.md` - Detailed Dr Tin mentions report\n")
            f.write("- `reports/enhanced_crawl_summary.md` - This summary\n")
            f.write("- `logs/enhanced_hko_crawler.log` - Detailed logs\n")
        
        # Generate content analysis report
        self.content_analyzer.generate_analysis_report()
        self.content_analyzer.save_analysis_data()
        
        # Update the main log file
        self.update_main_log()
        
        self.logger.info(f"Comprehensive reports generated in: {self.output_dir / 'reports'}")
    
    def update_main_log(self):
        """Update the main log file with crawl results"""
        log_file = self.output_dir / "webCrawllog_HKO.md"
        
        dr_tin_pages = [p for p in self.site_map if p.get('has_dr_tin_mention', False)]
        high_relevance_pages = [p for p in self.site_map if p.get('relevance_score', 0) > 0.5]
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Web Crawl Log - Enhanced Analysis\n\n")
            f.write(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Crawl Summary\n\n")
            f.write(f"- **Total Pages Crawled:** {len(self.visited_urls)}\n")
            f.write(f"- **Dr Tin Chatbot Mentions Found:** {len(dr_tin_pages)}\n")
            f.write(f"- **High Relevance Pages:** {len(high_relevance_pages)}\n")
            f.write(f"- **Failed Downloads:** {len(self.failed_urls)}\n\n")
            
            if dr_tin_pages:
                f.write("## Dr Tin Chatbot Mentions Found\n\n")
                for i, page in enumerate(dr_tin_pages, 1):
                    f.write(f"{i}. [{page['url']}]({page['url']}) (Score: {page.get('relevance_score', 0):.2f})\n")
            else:
                f.write("## No Dr Tin Chatbot Mentions Found\n\n")
                f.write("The enhanced crawler did not find any pages mentioning 'Dr Tin chatbot' on the HKO website.\n")
                f.write("However, check the high relevance pages for related content.\n\n")
            
            if high_relevance_pages:
                f.write("## High Relevance Pages (May Contain Related Content)\n\n")
                for i, page in enumerate(high_relevance_pages, 1):
                    f.write(f"{i}. [{page['url']}]({page['url']}) (Score: {page.get('relevance_score', 0):.2f})\n")
            
            f.write(f"\n## Files Generated\n\n")
            f.write(f"- Downloaded pages: `downloaded_pages/`\n")
            f.write(f"- Dr Tin mentions: `dr_tin_mentions/`\n")
            f.write(f"- High relevance pages: `high_relevance_pages/`\n")
            f.write(f"- Reports: `reports/`\n")
            f.write(f"- Detailed logs: `logs/enhanced_hko_crawler.log`\n")

def main():
    """Main function to run the enhanced HKO crawler"""
    print("Starting Enhanced HKO Web Crawler...")
    print("Performing advanced content analysis for Dr Tin chatbot mentions")
    print("=" * 60)
    
    crawler = EnhancedHKOWebCrawler()
    
    try:
        crawler.crawl(max_pages=500, delay=1)
        
        print("\n" + "=" * 60)
        print("ENHANCED HKO CRAWL COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Total pages crawled: {len(crawler.visited_urls)}")
        
        dr_tin_count = len([p for p in crawler.site_map if p.get('has_dr_tin_mention', False)])
        high_relevance_count = len([p for p in crawler.site_map if p.get('relevance_score', 0) > 0.5])
        
        print(f"Dr Tin chatbot mentions found: {dr_tin_count}")
        print(f"High relevance pages: {high_relevance_count}")
        print(f"Files saved to: {crawler.output_dir}")
        print("\nGenerated files:")
        print(f"  - downloaded_pages/ (all HTML pages)")
        print(f"  - dr_tin_mentions/ (pages with Dr Tin mentions)")
        print(f"  - high_relevance_pages/ (high relevance content)")
        print(f"  - reports/ (comprehensive analysis reports)")
        print(f"  - webCrawllog_HKO.md (main log)")
        print(f"  - logs/ (detailed logs)")
        
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


