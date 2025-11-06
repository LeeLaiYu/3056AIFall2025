#!/usr/bin/env python3
"""
HKO Direct Website Scraper
Searches the Hong Kong Observatory website directly for datasets
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import csv
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HKODirectWebsiteScraper:
    def __init__(self):
        self.hko_base_url = "https://www.hko.gov.hk"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.datasets = []
        
        # Potential HKO data URLs to check
        self.potential_data_urls = [
            "https://www.hko.gov.hk/en/datasets/",
            "https://www.hko.gov.hk/en/data/",
            "https://www.hko.gov.hk/en/open-data/",
            "https://www.hko.gov.hk/en/api/",
            "https://www.hko.gov.hk/en/weather/",
            "https://www.hko.gov.hk/en/climate/",
            "https://www.hko.gov.hk/en/forecast/",
            "https://www.hko.gov.hk/en/observation/",
            "https://www.hko.gov.hk/en/marine/",
            "https://www.hko.gov.hk/en/aviation/",
            "https://www.hko.gov.hk/en/radiation/",
            "https://www.hko.gov.hk/en/air-quality/",
            "https://www.hko.gov.hk/en/uv-index/",
            "https://www.hko.gov.hk/en/tide/",
            "https://www.hko.gov.hk/en/typhoon/",
            "https://www.hko.gov.hk/en/warning/",
            "https://www.hko.gov.hk/en/radar/",
            "https://www.hko.gov.hk/en/satellite/",
            "https://www.hko.gov.hk/en/lightning/",
            "https://www.hko.gov.hk/en/rainfall/"
        ]
    
    def get_page_content(self, url, max_retries=3):
        """Get page content with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def search_hko_website(self):
        """Search the HKO website for data-related pages"""
        logger.info("Searching HKO website for data-related content...")
        
        found_pages = []
        
        for url in self.potential_data_urls:
            logger.info(f"Checking: {url}")
            response = self.get_page_content(url)
            
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if this page contains data-related content
                page_text = soup.get_text().lower()
                data_indicators = [
                    'dataset', 'data', 'download', 'api', 'csv', 'json', 'xml',
                    'weather data', 'climate data', 'meteorological data',
                    'open data', 'public data', 'historical data'
                ]
                
                if any(indicator in page_text for indicator in data_indicators):
                    found_pages.append({
                        'url': url,
                        'title': soup.title.string if soup.title else 'Unknown',
                        'content_length': len(page_text)
                    })
                    logger.info(f"✅ Found data-related content: {url}")
                else:
                    logger.info(f"❌ No data content found: {url}")
            else:
                logger.info(f"❌ Page not accessible: {url}")
            
            time.sleep(1)  # Be respectful
        
        return found_pages
    
    def find_data_links(self, base_url):
        """Find data-related links on a page"""
        response = self.get_page_content(base_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        data_links = []
        
        # Look for links that might contain data
        link_selectors = [
            'a[href*="data"]', 'a[href*="dataset"]', 'a[href*="download"]',
            'a[href*="api"]', 'a[href*="csv"]', 'a[href*="json"]',
            'a[href*="xml"]', 'a[href*="weather"]', 'a[href*="climate"]'
        ]
        
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    full_url = urljoin(base_url, href)
                    link_text = link.get_text().strip()
                    data_links.append({
                        'url': full_url,
                        'text': link_text,
                        'type': 'data_link'
                    })
        
        return data_links
    
    def generate_report(self, found_pages, data_links):
        """Generate a report of findings"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate markdown report
        md_file = f"hko_direct_website_findings_{timestamp}.md"
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# Hong Kong Observatory Direct Website Analysis\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Base URL:** {self.hko_base_url}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write("This report analyzes the Hong Kong Observatory website to find data-related content and potential dataset sources.\n\n")
            
            f.write(f"## Found Data-Related Pages ({len(found_pages)})\n\n")
            for i, page in enumerate(found_pages, 1):
                f.write(f"### {i}. {page['title']}\n")
                f.write(f"**URL:** {page['url']}\n")
                f.write(f"**Content Length:** {page['content_length']} characters\n\n")
            
            f.write(f"## Data Links Found ({len(data_links)})\n\n")
            for i, link in enumerate(data_links, 1):
                f.write(f"{i}. **{link['text']}**\n")
                f.write(f"   - URL: {link['url']}\n")
                f.write(f"   - Type: {link['type']}\n\n")
            
            f.write("## Recommendations\n\n")
            f.write("1. **Check Found Pages**: Visit the data-related pages found above\n")
            f.write("2. **Explore Data Links**: Follow the data links to find actual datasets\n")
            f.write("3. **Contact HKO**: Reach out to HKO directly for dataset access\n")
            f.write("4. **API Documentation**: Look for HKO API documentation\n")
            f.write("5. **Alternative Sources**: Check other government data portals\n\n")
            
            f.write("## Next Steps\n\n")
            f.write("1. Visit each found page to look for downloadable datasets\n")
            f.write("2. Check for API endpoints and documentation\n")
            f.write("3. Look for data download sections\n")
            f.write("4. Contact HKO for direct dataset access\n")
        
        logger.info(f"Report saved: {md_file}")
        return md_file
    
    def run_analysis(self):
        """Run the complete analysis"""
        logger.info("Starting HKO direct website analysis...")
        
        try:
            # Search for data-related pages
            found_pages = self.search_hko_website()
            
            # Find data links from the main page
            data_links = self.find_data_links(self.hko_base_url)
            
            # Generate report
            report_file = self.generate_report(found_pages, data_links)
            
            logger.info("Analysis completed!")
            logger.info(f"Found {len(found_pages)} data-related pages")
            logger.info(f"Found {len(data_links)} data links")
            logger.info(f"Report saved: {report_file}")
            
            return {
                'found_pages': found_pages,
                'data_links': data_links,
                'report_file': report_file
            }
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            return None

def main():
    """Main function to run the scraper"""
    scraper = HKODirectWebsiteScraper()
    result = scraper.run_analysis()
    
    if result:
        print(f"\n✅ Analysis completed!")
        print(f"📊 Found {len(result['found_pages'])} data-related pages")
        print(f"🔗 Found {len(result['data_links'])} data links")
        print(f"📁 Report saved: {result['report_file']}")
    else:
        print("❌ Analysis failed. Check the logs for details.")

if __name__ == "__main__":
    main()


