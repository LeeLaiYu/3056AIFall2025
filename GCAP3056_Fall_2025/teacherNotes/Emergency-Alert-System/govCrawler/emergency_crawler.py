#!/usr/bin/env python3
"""
Hong Kong Government Emergency Directory Crawler
Crawls the government telephone directory to find emergency-related pages
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import os

class EmergencyDirectoryCrawler:
    def __init__(self, base_url="https://tel.directory.gov.hk/", output_dir=None):
        self.base_url = base_url
        self.output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
        self.visited_urls = set()
        self.emergency_pages = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup logging
        log_file = os.path.join(self.output_dir, 'emergency_crawler.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Comprehensive emergency-related keywords
        self.emergency_keywords = [
            # Core emergency terms
            'emergency', 'crisis', 'disaster', 'alert', 'warning', 'preparedness',
            'response', 'rescue', 'safety', 'security', 'incident', 'hazard',
            'evacuation', 'shelter', 'relief', 'assistance', 'support', 'coordination',
            'management', 'control', 'monitoring', 'assessment', 'planning',
            
            # Weather and natural disasters
            'typhoon', 'hurricane', 'flood', 'earthquake', 'tsunami', 'storm',
            'weather', 'meteorological', 'observatory', 'climate', 'seismic',
            
            # Public safety and health
            'public health', 'epidemic', 'pandemic', 'outbreak', 'quarantine',
            'medical', 'hospital', 'ambulance', 'paramedic', 'healthcare',
            
            # Infrastructure and utilities
            'infrastructure', 'power', 'electricity', 'water', 'gas', 'telecommunications',
            'transport', 'traffic', 'aviation', 'maritime', 'port', 'airport',
            
            # Government and administration
            'government', 'administration', 'bureau', 'department', 'agency',
            'authority', 'commission', 'council', 'office', 'secretariat',
            
            # Communication and information
            'communication', 'information', 'broadcast', 'media', 'news',
            'notification', 'announcement', 'publication', 'report',
            
            # Specific Hong Kong terms
            'hong kong', 'hksar', 'hk', 'government', 'civil', 'defence',
            'military', 'police', 'fire', 'medical', 'social', 'welfare',
            
            # Technical and operational
            'operation', 'system', 'network', 'database', 'information',
            'technology', 'digital', 'electronic', 'automated', 'system',
            
            # Time-sensitive terms
            'urgent', 'immediate', 'critical', 'priority', 'essential',
            'vital', 'important', 'significant', 'major', 'severe'
        ]

    def is_emergency_related(self, text, title=""):
        """Check if content is emergency-related based on keywords with scoring"""
        text_lower = (text + " " + title).lower()
        
        # Count keyword matches
        matches = []
        for keyword in self.emergency_keywords:
            if keyword in text_lower:
                matches.append(keyword)
        
        # Return matches if any found
        return matches if matches else None

    def crawl_page(self, url, max_depth=5, current_depth=0):
        """Crawl a single page and extract emergency-related information"""
        if url in self.visited_urls or current_depth > max_depth:
            return
        
        self.visited_urls.add(url)
        self.logger.info(f"Crawling: {url} (depth: {current_depth})")
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract page information
            title = soup.find('title')
            title_text = title.get_text().strip() if title else ""
            
            # Get all text content
            page_text = soup.get_text()
            
            # Check if page is emergency-related
            keyword_matches = self.is_emergency_related(page_text, title_text)
            if keyword_matches:
                # Extract contact information
                contacts = self.extract_contact_info(soup)
                
                emergency_page = {
                    'url': url,
                    'title': title_text,
                    'contacts': contacts,
                    'crawled_at': datetime.now().isoformat(),
                    'depth': current_depth,
                    'keyword_matches': keyword_matches,
                    'match_count': len(keyword_matches)
                }
                
                self.emergency_pages.append(emergency_page)
                self.logger.info(f"Found emergency-related page: {title_text} (Keywords: {', '.join(keyword_matches[:3])})")
                
                # Update log file
                self.update_log_file(f"Found emergency page: {title_text} - {url} (Keywords: {', '.join(keyword_matches)})")
            
            # Find links to crawl next
            if current_depth < max_depth:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    full_url = urljoin(url, href)
                    
                    # Only crawl pages from the same domain
                    if urlparse(full_url).netloc == urlparse(self.base_url).netloc:
                        time.sleep(1)  # Be respectful with requests
                        self.crawl_page(full_url, max_depth, current_depth + 1)
                        
        except Exception as e:
            self.logger.error(f"Error crawling {url}: {str(e)}")

    def extract_contact_info(self, soup):
        """Extract contact information from the page"""
        contacts = []
        
        # Look for phone numbers
        phone_pattern = r'(\+?852[-.\s]?)?[0-9]{4}[-.\s]?[0-9]{4}'
        phone_matches = re.findall(phone_pattern, soup.get_text())
        
        # Look for email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_matches = re.findall(email_pattern, soup.get_text())
        
        # Look for department/office names
        dept_elements = soup.find_all(['h1', 'h2', 'h3', 'strong', 'b'])
        departments = [elem.get_text().strip() for elem in dept_elements if elem.get_text().strip()]
        
        return {
            'phones': list(set(phone_matches)),
            'emails': list(set(email_matches)),
            'departments': departments[:5]  # Limit to first 5
        }

    def update_log_file(self, message):
        """Update the log file with progress"""
        log_file = os.path.join(self.output_dir, 'govCrawllog.md')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{timestamp}: {message}")

    def save_to_csv(self):
        """Save results to CSV file"""
        csv_file = os.path.join(self.output_dir, 'emergency_directory_results.csv')
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            if self.emergency_pages:
                writer = csv.DictWriter(f, fieldnames=self.emergency_pages[0].keys())
                writer.writeheader()
                writer.writerows(self.emergency_pages)
        
        self.logger.info(f"Results saved to {csv_file}")

    def run_crawl(self):
        """Main crawling function"""
        self.logger.info("Starting enhanced emergency directory crawl...")
        self.update_log_file("Starting enhanced emergency directory crawl with deeper search...")
        
        # Priority emergency-related starting points
        priority_urls = [
            self.base_url,
            "https://tel.directory.gov.hk/0262000019_ENG.html",  # Emergency Preparedness and Assessment
            "https://tel.directory.gov.hk/index_HKO_ENG.html",   # Hong Kong Observatory
            "https://tel.directory.gov.hk/index_FSD_ENG.html",   # Fire Services Department
            "https://tel.directory.gov.hk/index_HKPF_ENG.html",  # Hong Kong Police Force
            "https://tel.directory.gov.hk/index_SB_ENG.html",   # Security Bureau
            "https://tel.directory.gov.hk/index_HEALTH_ENG.html", # Health Bureau
            "https://tel.directory.gov.hk/index_DH_ENG.html",   # Department of Health
            "https://tel.directory.gov.hk/index_AMS_ENG.html",  # Auxiliary Medical Service
            "https://tel.directory.gov.hk/index_CAS_ENG.html",  # Civil Aid Service
            "https://tel.directory.gov.hk/index_GFS_ENG.html",  # Government Flying Service
            "https://tel.directory.gov.hk/index_EMSD_ENG.html", # Electrical & Mechanical Services Department
            "https://tel.directory.gov.hk/index_DSD_ENG.html",  # Drainage Services Department
            "https://tel.directory.gov.hk/index_WSD_ENG.html",  # Water Supplies Department
            "https://tel.directory.gov.hk/index_TD_ENG.html",   # Transport Department
            "https://tel.directory.gov.hk/index_HYD_ENG.html",  # Highways Department
            "https://tel.directory.gov.hk/index_MD_ENG.html",  # Marine Department
            "https://tel.directory.gov.hk/index_CAD_ENG.html",  # Civil Aviation Department
        ]
        
        # Crawl priority URLs first
        for url in priority_urls:
            self.logger.info(f"Crawling priority URL: {url}")
            self.crawl_page(url, max_depth=5, current_depth=0)
            time.sleep(2)  # Be respectful with requests
        
        # Save results
        self.save_to_csv()
        
        self.logger.info(f"Enhanced crawling completed. Found {len(self.emergency_pages)} emergency-related pages.")
        self.update_log_file(f"Enhanced crawling completed. Found {len(self.emergency_pages)} emergency-related pages.")

if __name__ == "__main__":
    crawler = EmergencyDirectoryCrawler()
    crawler.run_crawl()
