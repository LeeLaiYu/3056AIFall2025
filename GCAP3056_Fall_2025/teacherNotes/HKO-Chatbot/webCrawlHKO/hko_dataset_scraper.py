#!/usr/bin/env python3
"""
HKO Dataset Scraper
Scrapes all datasets from Hong Kong Observatory on data.gov.hk
Generates a comprehensive report of all available datasets
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

class HKODatasetScraper:
    def __init__(self):
        self.base_url = "https://data.gov.hk"
        self.hko_url = "https://data.gov.hk/en-datasets/provider/hk-hko"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.datasets = []
        
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
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def scrape_dataset_list(self):
        """Scrape the main HKO datasets page to get all dataset links"""
        logger.info("Starting to scrape HKO dataset list...")
        
        response = self.get_page_content(self.hko_url)
        if not response:
            logger.error("Failed to fetch main HKO page")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        dataset_links = []
        
        # Look for dataset links - these could be in various formats
        # Check for common patterns in data.gov.hk
        link_selectors = [
            'a[href*="/dataset/"]',
            'a[href*="/en-datasets/"]',
            '.dataset-item a',
            '.result-item a',
            'a[href*="hk-hko"]'
        ]
        
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href and ('dataset' in href or 'hk-hko' in href):
                    full_url = urljoin(self.base_url, href)
                    if full_url not in dataset_links:
                        dataset_links.append(full_url)
        
        logger.info(f"Found {len(dataset_links)} potential dataset links")
        return dataset_links
    
    def scrape_dataset_details(self, dataset_url):
        """Scrape detailed information from a single dataset page"""
        logger.info(f"Scraping dataset: {dataset_url}")
        
        response = self.get_page_content(dataset_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract dataset information
        dataset_info = {
            'url': dataset_url,
            'title': '',
            'description': '',
            'organization': 'Hong Kong Observatory',
            'tags': [],
            'formats': [],
            'last_updated': '',
            'license': '',
            'resources': []
        }
        
        # Extract title
        title_selectors = ['h1', '.page-header h1', '.dataset-title', 'title']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem and title_elem.get_text().strip():
                dataset_info['title'] = title_elem.get_text().strip()
                break
        
        # Extract description
        desc_selectors = ['.notes', '.description', '.dataset-description', 'meta[name="description"]']
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                if selector.startswith('meta'):
                    dataset_info['description'] = desc_elem.get('content', '').strip()
                else:
                    dataset_info['description'] = desc_elem.get_text().strip()
                if dataset_info['description']:
                    break
        
        # Extract tags/keywords
        tag_selectors = ['.tag', '.keyword', '.tag-list a', '.tags a']
        for selector in tag_selectors:
            tags = soup.select(selector)
            for tag in tags:
                tag_text = tag.get_text().strip()
                if tag_text and tag_text not in dataset_info['tags']:
                    dataset_info['tags'].append(tag_text)
        
        # Extract resources (downloadable files)
        resource_selectors = [
            'a[href*=".csv"]', 'a[href*=".json"]', 'a[href*=".xml"]', 
            'a[href*=".xlsx"]', 'a[href*=".pdf"]', '.resource-item a',
            'a[href*="download"]', 'a[href*="api"]'
        ]
        
        for selector in resource_selectors:
            resources = soup.select(selector)
            for resource in resources:
                href = resource.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    resource_name = resource.get_text().strip() or urlparse(href).path.split('/')[-1]
                    dataset_info['resources'].append({
                        'name': resource_name,
                        'url': full_url,
                        'format': urlparse(href).path.split('.')[-1].lower() if '.' in href else 'unknown'
                    })
        
        # Extract last updated date
        date_selectors = ['.date', '.last-updated', '.modified', 'time']
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_text = date_elem.get_text().strip() or date_elem.get('datetime', '').strip()
                if date_text:
                    dataset_info['last_updated'] = date_text
                    break
        
        # Extract license information
        license_selectors = ['.license', '.rights', '.terms']
        for selector in license_selectors:
            license_elem = soup.select_one(selector)
            if license_elem:
                dataset_info['license'] = license_elem.get_text().strip()
                break
        
        return dataset_info
    
    def scrape_all_datasets(self):
        """Main method to scrape all HKO datasets"""
        logger.info("Starting comprehensive HKO dataset scraping...")
        
        # Get list of all dataset URLs
        dataset_urls = self.scrape_dataset_list()
        
        if not dataset_urls:
            logger.warning("No dataset URLs found. Trying alternative approach...")
            # Try to find datasets through pagination or API
            dataset_urls = self.find_datasets_alternative()
        
        logger.info(f"Processing {len(dataset_urls)} datasets...")
        
        # Scrape each dataset
        for i, url in enumerate(dataset_urls, 1):
            logger.info(f"Processing dataset {i}/{len(dataset_urls)}")
            
            dataset_info = self.scrape_dataset_details(url)
            if dataset_info:
                self.datasets.append(dataset_info)
                logger.info(f"Successfully scraped: {dataset_info['title']}")
            else:
                logger.warning(f"Failed to scrape dataset: {url}")
            
            # Be respectful - add delay between requests
            time.sleep(1)
        
        logger.info(f"Scraping completed. Found {len(self.datasets)} datasets.")
        return self.datasets
    
    def find_datasets_alternative(self):
        """Alternative method to find datasets if main scraping fails"""
        logger.info("Trying alternative dataset discovery methods...")
        
        # Try to find datasets through search or API endpoints
        search_urls = [
            f"{self.base_url}/en-datasets?q=weather",
            f"{self.base_url}/en-datasets?q=climate",
            f"{self.base_url}/en-datasets?q=meteorological",
            f"{self.base_url}/en-datasets?organization=hk-hko"
        ]
        
        all_links = []
        for search_url in search_urls:
            response = self.get_page_content(search_url)
            if response:
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.select('a[href*="/dataset/"]')
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in all_links:
                            all_links.append(full_url)
        
        return all_links
    
    def generate_report(self, output_format='both'):
        """Generate comprehensive report of all datasets"""
        logger.info("Generating comprehensive dataset report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format in ['json', 'both']:
            json_file = f"hko_datasets_report_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.datasets, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON report saved: {json_file}")
        
        if output_format in ['csv', 'both']:
            csv_file = f"hko_datasets_report_{timestamp}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                if self.datasets:
                    writer = csv.DictWriter(f, fieldnames=self.datasets[0].keys())
                    writer.writeheader()
                    for dataset in self.datasets:
                        # Flatten nested structures for CSV
                        flat_dataset = dataset.copy()
                        flat_dataset['tags'] = '; '.join(dataset['tags'])
                        flat_dataset['resources'] = '; '.join([f"{r['name']} ({r['format']})" for r in dataset['resources']])
                        writer.writerow(flat_dataset)
            logger.info(f"CSV report saved: {csv_file}")
        
        # Generate markdown report
        md_file = f"hko_datasets_report_{timestamp}.md"
        self.generate_markdown_report(md_file)
        logger.info(f"Markdown report saved: {md_file}")
        
        return {
            'total_datasets': len(self.datasets),
            'json_file': json_file if output_format in ['json', 'both'] else None,
            'csv_file': csv_file if output_format in ['csv', 'both'] else None,
            'markdown_file': md_file
        }
    
    def generate_markdown_report(self, filename):
        """Generate a detailed markdown report"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("# Hong Kong Observatory Datasets Report\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Total Datasets:** {len(self.datasets)}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write(f"This report provides a comprehensive overview of all {len(self.datasets)} datasets ")
            f.write("available from the Hong Kong Observatory through the data.gov.hk portal.\n\n")
            
            # Dataset categories analysis
            categories = {}
            for dataset in self.datasets:
                for tag in dataset['tags']:
                    categories[tag] = categories.get(tag, 0) + 1
            
            if categories:
                f.write("## Dataset Categories\n\n")
                for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"- **{category}**: {count} datasets\n")
                f.write("\n")
            
            # Detailed dataset information
            f.write("## Detailed Dataset Information\n\n")
            for i, dataset in enumerate(self.datasets, 1):
                f.write(f"### {i}. {dataset['title']}\n\n")
                f.write(f"**URL:** {dataset['url']}\n\n")
                
                if dataset['description']:
                    f.write(f"**Description:** {dataset['description']}\n\n")
                
                if dataset['tags']:
                    f.write(f"**Tags:** {', '.join(dataset['tags'])}\n\n")
                
                if dataset['last_updated']:
                    f.write(f"**Last Updated:** {dataset['last_updated']}\n\n")
                
                if dataset['resources']:
                    f.write("**Available Resources:**\n")
                    for resource in dataset['resources']:
                        f.write(f"- [{resource['name']}]({resource['url']}) ({resource['format'].upper()})\n")
                    f.write("\n")
                
                if dataset['license']:
                    f.write(f"**License:** {dataset['license']}\n\n")
                
                f.write("---\n\n")
    
    def run_full_scrape(self):
        """Run the complete scraping process"""
        logger.info("Starting full HKO dataset scraping process...")
        
        try:
            # Scrape all datasets
            datasets = self.scrape_all_datasets()
            
            if not datasets:
                logger.error("No datasets found. Please check the website structure.")
                return None
            
            # Generate reports
            report_files = self.generate_report('both')
            
            logger.info("Scraping process completed successfully!")
            logger.info(f"Found {len(datasets)} datasets")
            logger.info(f"Report files generated: {report_files}")
            
            return {
                'datasets': datasets,
                'report_files': report_files,
                'total_count': len(datasets)
            }
            
        except Exception as e:
            logger.error(f"Error during scraping process: {e}")
            return None

def main():
    """Main function to run the scraper"""
    scraper = HKODatasetScraper()
    result = scraper.run_full_scrape()
    
    if result:
        print(f"\n✅ Scraping completed successfully!")
        print(f"📊 Total datasets found: {result['total_count']}")
        print(f"📁 Report files generated:")
        for file_type, file_path in result['report_files'].items():
            if file_path:
                print(f"   - {file_type}: {file_path}")
    else:
        print("❌ Scraping failed. Check the logs for details.")

if __name__ == "__main__":
    main()



