#!/usr/bin/env python3
"""
Content Analyzer for CyberDefender.hk
Visits each discovered URL and extracts one-line summaries
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
from pathlib import Path
from datetime import datetime
import re

class ContentAnalyzer:
    def __init__(self, output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\Anti-Scamming\\cytberdefender"):
        self.output_dir = Path(output_dir)
        self.sitemap_file = self.output_dir / "sitemap.csv"
        self.urls_file = self.output_dir / "all_discovered_urls.txt"
        self.enhanced_sitemap_file = self.output_dir / "enhanced_sitemap.csv"
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / "content_analyzer.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_page_summary(self, url):
        """Extract a one-line summary from a webpage"""
        try:
            self.logger.info(f"Analyzing: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to extract a meaningful summary
            summary = self.generate_summary(soup, url)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to analyze {url}: {str(e)}")
            return f"Error: {str(e)[:100]}"
    
    def generate_summary(self, soup, url):
        """Generate a one-line summary from page content"""
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Try different methods to get meaningful content
        summary_parts = []
        
        # Method 1: Look for main heading
        main_heading = soup.find(['h1', 'h2'])
        if main_heading:
            heading_text = main_heading.get_text().strip()
            if len(heading_text) > 10 and len(heading_text) < 200:
                summary_parts.append(heading_text)
        
        # Method 2: Look for meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc.get('content').strip()
            if len(desc) > 10 and len(desc) < 200:
                summary_parts.append(desc)
        
        # Method 3: Look for first paragraph with substantial content
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 50 and len(text) < 200:
                # Clean up the text
                text = re.sub(r'\s+', ' ', text)
                summary_parts.append(text)
                break
        
        # Method 4: Look for specific content based on URL patterns
        if '/scameter' in url:
            summary_parts.append("Scameter tool for checking suspicious websites and phone numbers")
        elif '/phishing' in url:
            summary_parts.append("Information about phishing attacks and how to protect yourself")
        elif '/ransomware' in url:
            summary_parts.append("Guidance on ransomware threats and prevention measures")
        elif '/cryptocurrency' in url:
            summary_parts.append("Educational content about cryptocurrency and related risks")
        elif '/cyberbullying' in url:
            summary_parts.append("Resources about cyberbullying prevention and response")
        elif '/child' in url:
            summary_parts.append("Child protection resources and online safety guidelines")
        elif '/privacy' in url:
            summary_parts.append("Privacy protection tips and digital security advice")
        elif '/password' in url:
            summary_parts.append("Password security tools and best practices")
        elif '/firewall' in url:
            summary_parts.append("Firewall setup and network security guidance")
        elif '/wifi' in url:
            summary_parts.append("Public WiFi safety tips and security measures")
        
        # Combine the best summary
        if summary_parts:
            # Take the first meaningful summary
            summary = summary_parts[0]
            # Clean up the summary
            summary = re.sub(r'\s+', ' ', summary)
            summary = summary.strip()
            
            # Truncate if too long
            if len(summary) > 200:
                summary = summary[:197] + "..."
            
            return summary
        else:
            # Fallback: use page title
            title = soup.find('title')
            if title:
                return title.get_text().strip()[:200]
            else:
                return "Content analysis completed - no specific summary available"
    
    def read_existing_sitemap(self):
        """Read the existing sitemap CSV"""
        sitemap_data = []
        with open(self.sitemap_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sitemap_data.append(row)
        return sitemap_data
    
    def read_discovered_urls(self):
        """Read all discovered URLs"""
        urls = []
        with open(self.urls_file, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url:
                    urls.append(url)
        return urls
    
    def analyze_all_content(self):
        """Analyze content for all discovered URLs"""
        self.logger.info("Starting content analysis...")
        
        # Read existing sitemap
        sitemap_data = self.read_existing_sitemap()
        
        # Read discovered URLs
        discovered_urls = self.read_discovered_urls()
        
        # Create URL to summary mapping
        url_summaries = {}
        
        self.logger.info(f"Analyzing {len(discovered_urls)} URLs...")
        
        for i, url in enumerate(discovered_urls, 1):
            self.logger.info(f"Progress: {i}/{len(discovered_urls)} - {url}")
            
            # Check if we already have this URL in sitemap
            existing_entry = None
            for entry in sitemap_data:
                if entry['URL'] == url:
                    existing_entry = entry
                    break
            
            if existing_entry:
                # Use existing data
                summary = self.extract_page_summary(url)
                url_summaries[url] = summary
            else:
                # New URL, analyze it
                summary = self.extract_page_summary(url)
                url_summaries[url] = summary
            
            # Respectful delay
            time.sleep(1)
            
            if i % 10 == 0:
                self.logger.info(f"Completed {i} URLs, continuing...")
        
        # Create enhanced sitemap
        self.create_enhanced_sitemap(sitemap_data, url_summaries)
        
        self.logger.info("Content analysis completed!")
    
    def create_enhanced_sitemap(self, sitemap_data, url_summaries):
        """Create enhanced sitemap with summaries"""
        enhanced_file = self.output_dir / "enhanced_sitemap.csv"
        
        with open(enhanced_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'URL', 'Filename', 'Title', 'Links Found', 'Status', 
                'Timestamp', 'Error (if failed)', 'Category', 'Page Summary'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for entry in sitemap_data:
                url = entry['URL']
                summary = url_summaries.get(url, 'Summary not available')
                
                enhanced_entry = entry.copy()
                enhanced_entry['Page Summary'] = summary
                writer.writerow(enhanced_entry)
        
        self.logger.info(f"Enhanced sitemap saved to: {enhanced_file}")

def main():
    """Main function to run content analysis"""
    print("Starting Content Analysis for CyberDefender URLs...")
    print("=" * 60)
    
    analyzer = ContentAnalyzer()
    
    try:
        analyzer.analyze_all_content()
        
        print("\n" + "=" * 60)
        print("CONTENT ANALYSIS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Enhanced sitemap with summaries created:")
        print(f"  - enhanced_sitemap.csv")
        print(f"  - content_analyzer.log (detailed logs)")
        
    except KeyboardInterrupt:
        print("\nContent analysis interrupted by user.")
    except Exception as e:
        print(f"\nError during content analysis: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


