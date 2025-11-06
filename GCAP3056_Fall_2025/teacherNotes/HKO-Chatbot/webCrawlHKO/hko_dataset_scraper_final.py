#!/usr/bin/env python3
"""
Final HKO Dataset Scraper
Directly scrapes the HKO provider page and finds all datasets
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

class FinalHKODatasetScraper:
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
    
    def find_all_hko_datasets(self):
        """Find all HKO datasets by exploring the provider page and related pages"""
        logger.info("Finding all HKO datasets...")
        
        # First, get the main HKO provider page
        response = self.get_page_content(self.hko_url)
        if not response:
            logger.error("Failed to fetch main HKO page")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        dataset_links = []
        
        # Look for dataset links in various formats
        link_patterns = [
            'a[href*="/dataset/"]',
            'a[href*="/en-dataset/"]',
            '.dataset-item a',
            '.result-item a',
            '.search-result a',
            'a[href*="hk-hko"]'
        ]
        
        for pattern in link_patterns:
            links = soup.select(pattern)
            for link in links:
                href = link.get('href')
                if href and ('dataset' in href or 'hk-hko' in href):
                    full_url = urljoin(self.base_url, href)
                    if full_url not in dataset_links:
                        dataset_links.append(full_url)
        
        # Also try to find datasets through search
        search_urls = [
            f"{self.base_url}/en-datasets?q=weather&organization=hk-hko",
            f"{self.base_url}/en-datasets?q=climate&organization=hk-hko",
            f"{self.base_url}/en-datasets?q=meteorological&organization=hk-hko",
            f"{self.base_url}/en-datasets?organization=hk-hko"
        ]
        
        for search_url in search_urls:
            response = self.get_page_content(search_url)
            if response:
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.select('a[href*="/dataset/"]')
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in dataset_links:
                            dataset_links.append(full_url)
        
        # Try to find datasets through pagination
        page = 1
        while page <= 10:  # Limit to 10 pages to avoid infinite loops
            paginated_url = f"{self.base_url}/en-datasets?organization=hk-hko&page={page}"
            response = self.get_page_content(paginated_url)
            if not response:
                break
                
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.select('a[href*="/dataset/"]')
            
            if not links:  # No more datasets found
                break
                
            for link in links:
                href = link.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in dataset_links:
                        dataset_links.append(full_url)
            
            page += 1
            time.sleep(1)  # Be respectful
        
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
        title_selectors = [
            'h1', '.page-header h1', '.dataset-title', 'title',
            '.dataset-header h1', '.content-header h1'
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem and title_elem.get_text().strip():
                dataset_info['title'] = title_elem.get_text().strip()
                break
        
        # Extract description
        desc_selectors = [
            '.notes', '.description', '.dataset-description', 
            'meta[name="description"]', '.dataset-notes',
            '.content-description'
        ]
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
        tag_selectors = [
            '.tag', '.keyword', '.tag-list a', '.tags a',
            '.dataset-tags a', '.keyword-list a'
        ]
        for selector in tag_selectors:
            tags = soup.select(selector)
            for tag in tags:
                tag_text = tag.get_text().strip()
                if tag_text and tag_text not in dataset_info['tags']:
                    dataset_info['tags'].append(tag_text)
        
        # Extract author and maintainer information
        author_selectors = [
            '.author', '.dataset-author', '.content-author',
            'meta[name="author"]'
        ]
        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                if selector.startswith('meta'):
                    dataset_info['author'] = author_elem.get('content', '').strip()
                else:
                    dataset_info['author'] = author_elem.get_text().strip()
                if dataset_info['author']:
                    break
        
        # Extract resources (downloadable files)
        resource_selectors = [
            'a[href*=".csv"]', 'a[href*=".json"]', 'a[href*=".xml"]', 
            'a[href*=".xlsx"]', 'a[href*=".pdf"]', '.resource-item a',
            'a[href*="download"]', 'a[href*="api"]', '.resource a',
            '.download-link', '.file-link'
        ]
        
        for selector in resource_selectors:
            resources = soup.select(selector)
            for resource in resources:
                href = resource.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    resource_name = resource.get_text().strip() or urlparse(href).path.split('/')[-1]
                    resource_format = urlparse(href).path.split('.')[-1].lower() if '.' in href else 'unknown'
                    
                    dataset_info['resources'].append({
                        'name': resource_name,
                        'url': full_url,
                        'format': resource_format.upper()
                    })
                    
                    if resource_format not in dataset_info['formats']:
                        dataset_info['formats'].append(resource_format)
        
        # Extract dates
        date_selectors = [
            '.date', '.last-updated', '.modified', 'time',
            '.dataset-date', '.content-date'
        ]
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_text = date_elem.get_text().strip() or date_elem.get('datetime', '').strip()
                if date_text:
                    if 'created' in selector.lower() or 'created' in date_text.lower():
                        dataset_info['created'] = date_text
                    else:
                        dataset_info['last_updated'] = date_text
                    break
        
        # Extract license information
        license_selectors = [
            '.license', '.rights', '.terms', '.dataset-license'
        ]
        for selector in license_selectors:
            license_elem = soup.select_one(selector)
            if license_elem:
                dataset_info['license'] = license_elem.get_text().strip()
                break
        
        return dataset_info
    
    def scrape_all_datasets(self):
        """Main method to scrape all HKO datasets"""
        logger.info("Starting comprehensive HKO dataset scraping...")
        
        # Find all dataset URLs
        dataset_urls = self.find_all_hko_datasets()
        
        if not dataset_urls:
            logger.warning("No dataset URLs found. Trying alternative approach...")
            # Try alternative methods
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
            time.sleep(1)
        
        return all_links
    
    def generate_report(self, output_format='both'):
        """Generate comprehensive report of all datasets"""
        logger.info("Generating comprehensive dataset report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format in ['json', 'both']:
            json_file = f"hko_datasets_final_report_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.datasets, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON report saved: {json_file}")
        
        if output_format in ['csv', 'both']:
            csv_file = f"hko_datasets_final_report_{timestamp}.csv"
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
        md_file = f"hko_datasets_final_report_{timestamp}.md"
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
            f.write("# Hong Kong Observatory Datasets - Final Report\n\n")
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
    scraper = FinalHKODatasetScraper()
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



