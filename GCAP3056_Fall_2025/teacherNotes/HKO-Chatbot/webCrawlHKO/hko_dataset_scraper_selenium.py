#!/usr/bin/env python3
"""
HKO Dataset Scraper using Selenium
Handles JavaScript-rendered content to find all HKO datasets
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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumHKODatasetScraper:
    def __init__(self):
        self.base_url = "https://data.gov.hk"
        self.hko_url = "https://data.gov.hk/en-datasets/provider/hk-hko"
        self.driver = None
        self.datasets = []
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in background
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            logger.info("Trying alternative approach without Selenium...")
            return False
    
    def find_all_hko_datasets_selenium(self):
        """Find all HKO datasets using Selenium to handle JavaScript"""
        logger.info("Finding all HKO datasets using Selenium...")
        
        if not self.driver:
            logger.error("Driver not initialized")
            return []
        
        try:
            # Navigate to the HKO provider page
            self.driver.get(self.hko_url)
            logger.info(f"Navigated to: {self.hko_url}")
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "dataset-listing"))
            )
            
            # Wait for datasets to load
            time.sleep(5)
            
            # Find all dataset links
            dataset_links = []
            
            # Look for dataset links in various formats
            link_selectors = [
                'a[href*="/dataset/"]',
                'a[href*="/en-dataset/"]',
                '.dataset-listing__item-title',
                '.dataset-listing__item a'
            ]
            
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and ('dataset' in href or 'hk-hko' in href):
                            if href not in dataset_links:
                                dataset_links.append(href)
                except Exception as e:
                    logger.warning(f"Error with selector {selector}: {e}")
            
            # Also try to find datasets through search
            try:
                search_url = f"{self.base_url}/en-datasets?organization=hk-hko"
                self.driver.get(search_url)
                time.sleep(3)
                
                elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/dataset/"]')
                for element in elements:
                    href = element.get_attribute('href')
                    if href and href not in dataset_links:
                        dataset_links.append(href)
            except Exception as e:
                logger.warning(f"Error with search approach: {e}")
            
            logger.info(f"Found {len(dataset_links)} potential dataset links")
            return dataset_links
            
        except TimeoutException:
            logger.error("Timeout waiting for page to load")
            return []
        except Exception as e:
            logger.error(f"Error finding datasets: {e}")
            return []
    
    def scrape_dataset_details_selenium(self, dataset_url):
        """Scrape detailed information from a single dataset page using Selenium"""
        logger.info(f"Scraping dataset: {dataset_url}")
        
        try:
            self.driver.get(dataset_url)
            time.sleep(3)  # Wait for page to load
            
            # Extract dataset information
            dataset_info = {
                'url': dataset_url,
                'name': '',
                'title': '',
                'description': '',
                'organization': 'Hong Kong Observatory',
                'tags': [],
                'formats': [],
                'last_updated': '',
                'created': '',
                'license': '',
                'author': '',
                'maintainer': '',
                'resources': []
            }
            
            # Extract name from URL
            url_parts = dataset_url.split('/')
            if 'dataset' in url_parts:
                dataset_info['name'] = url_parts[url_parts.index('dataset') + 1]
            
            # Extract title
            try:
                title_element = self.driver.find_element(By.TAG_NAME, 'h1')
                dataset_info['title'] = title_element.text.strip()
            except NoSuchElementException:
                try:
                    title_element = self.driver.find_element(By.CSS_SELECTOR, '.page-title')
                    dataset_info['title'] = title_element.text.strip()
                except NoSuchElementException:
                    dataset_info['title'] = self.driver.title
            
            # Extract description
            try:
                desc_element = self.driver.find_element(By.CSS_SELECTOR, '.notes, .description, .dataset-description')
                dataset_info['description'] = desc_element.text.strip()
            except NoSuchElementException:
                pass
            
            # Extract tags
            try:
                tag_elements = self.driver.find_elements(By.CSS_SELECTOR, '.tag, .keyword, .tag-list a, .tags a')
                for tag_element in tag_elements:
                    tag_text = tag_element.text.strip()
                    if tag_text and tag_text not in dataset_info['tags']:
                        dataset_info['tags'].append(tag_text)
            except Exception as e:
                logger.warning(f"Error extracting tags: {e}")
            
            # Extract resources
            try:
                resource_elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*=".csv"], a[href*=".json"], a[href*=".xml"], a[href*=".xlsx"], a[href*=".pdf"]')
                for resource_element in resource_elements:
                    href = resource_element.get_attribute('href')
                    if href:
                        resource_name = resource_element.text.strip() or urlparse(href).path.split('/')[-1]
                        resource_format = urlparse(href).path.split('.')[-1].lower() if '.' in href else 'unknown'
                        
                        dataset_info['resources'].append({
                            'name': resource_name,
                            'url': href,
                            'format': resource_format.upper()
                        })
                        
                        if resource_format not in dataset_info['formats']:
                            dataset_info['formats'].append(resource_format)
            except Exception as e:
                logger.warning(f"Error extracting resources: {e}")
            
            return dataset_info
            
        except Exception as e:
            logger.error(f"Error scraping dataset {dataset_url}: {e}")
            return None
    
    def scrape_all_datasets(self):
        """Main method to scrape all HKO datasets"""
        logger.info("Starting comprehensive HKO dataset scraping using Selenium...")
        
        # Setup driver
        if not self.setup_driver():
            logger.error("Failed to setup driver, falling back to alternative method")
            return self.scrape_without_selenium()
        
        try:
            # Find all dataset URLs
            dataset_urls = self.find_all_hko_datasets_selenium()
            
            if not dataset_urls:
                logger.warning("No dataset URLs found with Selenium, trying alternative approach...")
                dataset_urls = self.find_datasets_alternative()
            
            logger.info(f"Processing {len(dataset_urls)} datasets...")
            
            # Scrape each dataset
            for i, url in enumerate(dataset_urls, 1):
                logger.info(f"Processing dataset {i}/{len(dataset_urls)}")
                
                dataset_info = self.scrape_dataset_details_selenium(url)
                if dataset_info:
                    self.datasets.append(dataset_info)
                    logger.info(f"Successfully scraped: {dataset_info['title']}")
                else:
                    logger.warning(f"Failed to scrape dataset: {url}")
                
                # Be respectful - add delay between requests
                time.sleep(2)
            
            logger.info(f"Scraping completed. Found {len(self.datasets)} datasets.")
            return self.datasets
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver closed")
    
    def scrape_without_selenium(self):
        """Fallback method without Selenium"""
        logger.info("Using fallback method without Selenium...")
        
        # Try to get datasets using direct HTTP requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Try different approaches to find datasets
        search_urls = [
            f"{self.base_url}/en-datasets?organization=hk-hko",
            f"{self.base_url}/en-datasets?q=weather&organization=hk-hko",
            f"{self.base_url}/en-datasets?q=climate&organization=hk-hko"
        ]
        
        all_links = []
        for search_url in search_urls:
            try:
                response = session.get(search_url, timeout=30)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    links = soup.select('a[href*="/dataset/"]')
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(self.base_url, href)
                            if full_url not in all_links:
                                all_links.append(full_url)
            except Exception as e:
                logger.warning(f"Error with {search_url}: {e}")
        
        logger.info(f"Found {len(all_links)} datasets using fallback method")
        return all_links
    
    def find_datasets_alternative(self):
        """Alternative method to find datasets"""
        logger.info("Trying alternative dataset discovery methods...")
        
        # Try different search approaches
        search_terms = [
            "weather", "climate", "meteorological", "temperature", "rainfall",
            "wind", "humidity", "pressure", "forecast", "observation"
        ]
        
        all_links = []
        for term in search_terms:
            search_url = f"{self.base_url}/en-datasets?q={term}&organization=hk-hko"
            try:
                response = requests.get(search_url, timeout=30)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    links = soup.select('a[href*="/dataset/"]')
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(self.base_url, href)
                            if full_url not in all_links:
                                all_links.append(full_url)
            except Exception as e:
                logger.warning(f"Error with search term {term}: {e}")
            time.sleep(1)
        
        return all_links
    
    def generate_report(self, output_format='both'):
        """Generate comprehensive report of all datasets"""
        logger.info("Generating comprehensive dataset report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format in ['json', 'both']:
            json_file = f"hko_datasets_selenium_report_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.datasets, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON report saved: {json_file}")
        
        if output_format in ['csv', 'both']:
            csv_file = f"hko_datasets_selenium_report_{timestamp}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                if self.datasets:
                    writer = csv.DictWriter(f, fieldnames=self.datasets[0].keys())
                    writer.writeheader()
                    for dataset in self.datasets:
                        # Flatten nested structures for CSV
                        flat_dataset = dataset.copy()
                        flat_dataset['tags'] = '; '.join(dataset['tags'])
                        flat_dataset['formats'] = '; '.join(dataset['formats'])
                        flat_dataset['resources'] = '; '.join([f"{r['name']} ({r['format']})" for r in dataset['resources']])
                        writer.writerow(flat_dataset)
            logger.info(f"CSV report saved: {csv_file}")
        
        # Generate markdown report
        md_file = f"hko_datasets_selenium_report_{timestamp}.md"
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
            f.write("# Hong Kong Observatory Datasets - Selenium Report\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Total Datasets:** {len(self.datasets)}\n")
            f.write(f"**Organization:** Hong Kong Observatory (hk-hko)\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write(f"This report provides a comprehensive overview of all {len(self.datasets)} datasets ")
            f.write("available from the Hong Kong Observatory through the data.gov.hk portal.\n\n")
            
            # Dataset statistics
            total_resources = sum(len(dataset['resources']) for dataset in self.datasets)
            f.write(f"**Total Resources:** {total_resources}\n\n")
            
            # Format analysis
            all_formats = []
            for dataset in self.datasets:
                all_formats.extend(dataset['formats'])
            
            format_counts = {}
            for fmt in all_formats:
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
            
            if format_counts:
                f.write("## Data Formats Available\n\n")
                for fmt, count in sorted(format_counts.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"- **{fmt.upper()}**: {count} datasets\n")
                f.write("\n")
            
            # Tag analysis
            all_tags = []
            for dataset in self.datasets:
                all_tags.extend(dataset['tags'])
            
            tag_counts = {}
            for tag in all_tags:
                if tag:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            if tag_counts:
                f.write("## Dataset Categories (Tags)\n\n")
                for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"- **{tag}**: {count} datasets\n")
                f.write("\n")
            
            # Detailed dataset information
            f.write("## Detailed Dataset Information\n\n")
            for i, dataset in enumerate(self.datasets, 1):
                f.write(f"### {i}. {dataset['title']}\n\n")
                f.write(f"**Dataset Name:** `{dataset['name']}`\n\n")
                f.write(f"**URL:** {dataset['url']}\n\n")
                
                if dataset['description']:
                    f.write(f"**Description:** {dataset['description']}\n\n")
                
                if dataset['tags']:
                    f.write(f"**Tags:** {', '.join(dataset['tags'])}\n\n")
                
                if dataset['author']:
                    f.write(f"**Author:** {dataset['author']}\n\n")
                
                if dataset['license']:
                    f.write(f"**License:** {dataset['license']}\n\n")
                
                if dataset['created']:
                    f.write(f"**Created:** {dataset['created']}\n\n")
                
                if dataset['last_updated']:
                    f.write(f"**Last Updated:** {dataset['last_updated']}\n\n")
                
                if dataset['resources']:
                    f.write(f"**Available Resources ({len(dataset['resources'])}):**\n\n")
                    for j, resource in enumerate(dataset['resources'], 1):
                        f.write(f"{j}. **{resource['name']}**\n")
                        f.write(f"   - Format: {resource['format']}\n")
                        f.write(f"   - URL: [{resource['url']}]({resource['url']})\n")
                        f.write("\n")
                
                f.write("---\n\n")
    
    def run_full_scrape(self):
        """Run the complete scraping process"""
        logger.info("Starting full HKO dataset scraping process using Selenium...")
        
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
    scraper = SeleniumHKODatasetScraper()
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



