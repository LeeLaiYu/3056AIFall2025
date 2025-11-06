#!/usr/bin/env python3
"""
Targeted Dr Tin Crawler
Specifically crawls the Dr Tin chatbot page and related content
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

class TargetedDrTinCrawler:
    def __init__(self, output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO"):
        self.output_dir = Path(output_dir)
        self.dr_tin_url = "https://www.hko.gov.hk/en/education/weather/data-and-technology/00569-How-Chatbot-Dr-Tin-is-Trained.html"
        self.target_urls = [
            self.dr_tin_url,
            "https://www.hko.gov.hk/en/education/weather/data-and-technology/",
            "https://www.hko.gov.hk/en/education/weather/",
            "https://www.hko.gov.hk/en/education/"
        ]
        
        # Create output directory structure
        self.setup_directories()
        
        # Setup logging
        self.setup_logging()
        
        self.crawled_pages = []
        self.dr_tin_content = None
        
    def setup_directories(self):
        """Create organized directory structure"""
        directories = [
            self.output_dir,
            self.output_dir / "targeted_crawl",
            self.output_dir / "dr_tin_analysis",
            self.output_dir / "reports"
        ]
        
        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_file = self.output_dir / "targeted_crawl" / "targeted_dr_tin_crawler.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def download_page(self, url):
        """Download a single page with enhanced error handling"""
        try:
            self.logger.info(f"Downloading: {url}")
            
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
            file_path = self.output_dir / "targeted_crawl" / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if self.is_valid_url(full_url):
                    links.append(full_url)
            
            # Analyze Dr Tin content
            dr_tin_analysis = self.analyze_dr_tin_content(content, url)
            
            page_info = {
                'url': url,
                'filename': filename,
                'status': 'success',
                'links_found': len(links),
                'timestamp': datetime.now().isoformat(),
                'links': links,
                'dr_tin_analysis': dr_tin_analysis
            }
            
            self.crawled_pages.append(page_info)
            
            if 'dr-tin' in url.lower() or '00569' in url:
                self.dr_tin_content = {
                    'url': url,
                    'content': content,
                    'html': response.text,
                    'analysis': dr_tin_analysis
                }
                self.save_dr_tin_analysis()
            
            return page_info
            
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            
            page_info = {
                'url': url,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            self.crawled_pages.append(page_info)
            return None
    
    def analyze_dr_tin_content(self, content, url):
        """Analyze content for Dr Tin specific information"""
        analysis = {
            'has_dr_tin_mention': False,
            'dr_tin_mentions': [],
            'technical_details': [],
            'statistics': [],
            'launch_info': [],
            'features': []
        }
        
        content_lower = content.lower()
        
        # Check for Dr Tin mentions
        dr_tin_patterns = [
            r'dr\s+tin',
            r'dr\.\s*tin',
            r'chatbot.*dr\s+tin',
            r'dr\s+tin.*chatbot'
        ]
        
        for pattern in dr_tin_patterns:
            matches = re.finditer(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                context_start = max(0, match.start() - 200)
                context_end = min(len(content), match.end() + 200)
                context = content[context_start:context_end].strip()
                
                analysis['dr_tin_mentions'].append({
                    'pattern': pattern,
                    'match': match.group(),
                    'context': context,
                    'position': match.start()
                })
                analysis['has_dr_tin_mention'] = True
        
        # Look for specific technical details
        technical_terms = [
            'artificial intelligence', 'ai', 'machine learning', 'nlp', 'nlu',
            'natural language', 'supervised learning', 'tokenization',
            'intent classification', 'entity extraction'
        ]
        
        for term in technical_terms:
            if term in content_lower:
                analysis['technical_details'].append(term)
        
        # Look for statistics
        stat_patterns = [
            r'(\d+)\s*thousands?\s*of\s*dialogues?',
            r'(\d+)\s*out\s*of\s*5',
            r'rating\s*of\s*(\d+)',
            r'(\d+)\s*monthly'
        ]
        
        for pattern in stat_patterns:
            matches = re.finditer(pattern, content_lower)
            for match in matches:
                analysis['statistics'].append({
                    'pattern': pattern,
                    'match': match.group(),
                    'value': match.group(1) if match.groups() else match.group()
                })
        
        # Look for launch information
        launch_patterns = [
            r'launched\s*in\s*(\w+\s*\d{4})',
            r'february\s*2020',
            r'started\s*in\s*(\w+\s*\d{4})'
        ]
        
        for pattern in launch_patterns:
            matches = re.finditer(pattern, content_lower)
            for match in matches:
                analysis['launch_info'].append(match.group())
        
        # Look for features
        feature_patterns = [
            r'weather\s*forecast',
            r'weather\s*warning',
            r'tidal\s*information',
            r'sunrise.*sunset',
            r'astronomy',
            r'current\s*weather'
        ]
        
        for pattern in feature_patterns:
            if re.search(pattern, content_lower):
                analysis['features'].append(pattern)
        
        return analysis
    
    def save_dr_tin_analysis(self):
        """Save detailed Dr Tin analysis"""
        if not self.dr_tin_content:
            return
        
        analysis_file = self.output_dir / "dr_tin_analysis" / "dr_tin_detailed_analysis.md"
        
        with open(analysis_file, 'w', encoding='utf-8') as f:
            f.write("# Dr Tin Chatbot - Detailed Analysis\n\n")
            f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Source URL:** {self.dr_tin_content['url']}\n\n")
            
            analysis = self.dr_tin_content['analysis']
            
            f.write("## Dr Tin Chatbot Information\n\n")
            
            if analysis['launch_info']:
                f.write("### Launch Information\n\n")
                for info in analysis['launch_info']:
                    f.write(f"- {info}\n")
                f.write("\n")
            
            if analysis['statistics']:
                f.write("### Statistics\n\n")
                for stat in analysis['statistics']:
                    f.write(f"- **{stat['pattern']}**: {stat['match']}\n")
                f.write("\n")
            
            if analysis['features']:
                f.write("### Features\n\n")
                for feature in analysis['features']:
                    f.write(f"- {feature}\n")
                f.write("\n")
            
            if analysis['technical_details']:
                f.write("### Technical Details\n\n")
                for detail in analysis['technical_details']:
                    f.write(f"- {detail}\n")
                f.write("\n")
            
            if analysis['dr_tin_mentions']:
                f.write("### Dr Tin Mentions\n\n")
                for i, mention in enumerate(analysis['dr_tin_mentions'], 1):
                    f.write(f"{i}. **Pattern:** `{mention['pattern']}`\n")
                    f.write(f"   **Match:** `{mention['match']}`\n")
                    f.write(f"   **Context:** {mention['context']}\n\n")
        
        # Save raw content
        content_file = self.output_dir / "dr_tin_analysis" / "dr_tin_raw_content.html"
        with open(content_file, 'w', encoding='utf-8') as f:
            f.write(self.dr_tin_content['html'])
        
        self.logger.info(f"Dr Tin analysis saved to: {analysis_file}")
    
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
                parsed.netloc == 'www.hko.gov.hk' and
                not any(ext in url.lower() for ext in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.xml', '.zip'])
            )
        except:
            return False
    
    def crawl_targeted_pages(self):
        """Crawl the specific Dr Tin page and related content"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING TARGETED DR TIN CRAWL")
        self.logger.info("=" * 60)
        
        for url in self.target_urls:
            self.logger.info(f"Targeting: {url}")
            page_info = self.download_page(url)
            
            if page_info and page_info['status'] == 'success':
                self.logger.info(f"Successfully crawled: {url}")
                
                # If this is the main Dr Tin page, also crawl related links
                if '00569' in url or 'dr-tin' in url.lower():
                    self.logger.info("Found Dr Tin page! Crawling related links...")
                    for link in page_info['links']:
                        if any(keyword in link.lower() for keyword in ['dr-tin', 'chatbot', 'ai', 'data-and-technology']):
                            self.logger.info(f"Following related link: {link}")
                            self.download_page(link)
                            time.sleep(1)  # Be respectful
            else:
                self.logger.error(f"Failed to crawl: {url}")
            
            time.sleep(1)  # Be respectful to the server
        
        # Generate comprehensive report
        self.generate_targeted_report()
        
        self.logger.info("=" * 60)
        self.logger.info("TARGETED DR TIN CRAWL COMPLETED")
        self.logger.info("=" * 60)
    
    def generate_targeted_report(self):
        """Generate comprehensive report for targeted crawl"""
        report_file = self.output_dir / "reports" / "targeted_dr_tin_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# Targeted Dr Tin Crawl Report\n\n")
            f.write(f"**Crawl Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Target URLs:** {len(self.target_urls)}\n")
            f.write(f"**Pages Crawled:** {len(self.crawled_pages)}\n\n")
            
            successful_pages = [p for p in self.crawled_pages if p['status'] == 'success']
            failed_pages = [p for p in self.crawled_pages if p['status'] == 'failed']
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Successful Downloads:** {len(successful_pages)}\n")
            f.write(f"- **Failed Downloads:** {len(failed_pages)}\n")
            
            dr_tin_pages = [p for p in successful_pages if p.get('dr_tin_analysis', {}).get('has_dr_tin_mention', False)]
            f.write(f"- **Dr Tin Mentions Found:** {len(dr_tin_pages)}\n\n")
            
            if dr_tin_pages:
                f.write("## Dr Tin Chatbot Pages Found\n\n")
                for page in dr_tin_pages:
                    f.write(f"### [{page['url']}]({page['url']})\n\n")
                    analysis = page.get('dr_tin_analysis', {})
                    
                    if analysis.get('launch_info'):
                        f.write("**Launch Information:**\n")
                        for info in analysis['launch_info']:
                            f.write(f"- {info}\n")
                        f.write("\n")
                    
                    if analysis.get('statistics'):
                        f.write("**Statistics:**\n")
                        for stat in analysis['statistics']:
                            f.write(f"- {stat['match']}\n")
                        f.write("\n")
                    
                    if analysis.get('features'):
                        f.write("**Features:**\n")
                        for feature in analysis['features']:
                            f.write(f"- {feature}\n")
                        f.write("\n")
            else:
                f.write("## No Dr Tin Mentions Found\n\n")
                f.write("The targeted crawl did not find any Dr Tin chatbot mentions.\n\n")
            
            f.write("## All Crawled Pages\n\n")
            for page in self.crawled_pages:
                status_icon = "✅" if page['status'] == 'success' else "❌"
                f.write(f"{status_icon} [{page['url']}]({page['url']}) - {page['status']}\n")
        
        # Generate CSV report
        csv_file = self.output_dir / "reports" / "targeted_dr_tin_sitemap.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['URL', 'Status', 'Links Found', 'Has Dr Tin Mention', 'Timestamp'])
            for page in self.crawled_pages:
                analysis = page.get('dr_tin_analysis', {})
                writer.writerow([
                    page['url'],
                    page['status'],
                    page.get('links_found', 0),
                    analysis.get('has_dr_tin_mention', False),
                    page.get('timestamp', '')
                ])
        
        self.logger.info(f"Targeted crawl report saved to: {report_file}")

def main():
    """Main function to run the targeted Dr Tin crawler"""
    print("Starting Targeted Dr Tin Crawler...")
    print("Focusing on the specific Dr Tin chatbot page and related content")
    print("=" * 60)
    
    crawler = TargetedDrTinCrawler()
    
    try:
        crawler.crawl_targeted_pages()
        
        print("\n" + "=" * 60)
        print("TARGETED DR TIN CRAWL COMPLETED!")
        print("=" * 60)
        print(f"Pages crawled: {len(crawler.crawled_pages)}")
        print(f"Dr Tin content found: {crawler.dr_tin_content is not None}")
        print(f"Files saved to: {crawler.output_dir}")
        print("\nGenerated files:")
        print(f"  - targeted_crawl/ (downloaded pages)")
        print(f"  - dr_tin_analysis/ (Dr Tin specific analysis)")
        print(f"  - reports/ (comprehensive reports)")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


