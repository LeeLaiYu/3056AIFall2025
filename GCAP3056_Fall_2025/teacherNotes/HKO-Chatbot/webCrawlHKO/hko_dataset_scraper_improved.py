#!/usr/bin/env python3
"""
Improved HKO Dataset Scraper
Uses CKAN API to get all HKO datasets from data.gov.hk
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

class ImprovedHKODatasetScraper:
    def __init__(self):
        self.base_url = "https://data.gov.hk"
        self.api_url = "https://data.gov.hk/api/3/action"
        self.hko_organization_id = "hk-hko"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.datasets = []
        
    def get_organization_datasets(self):
        """Get all datasets from HKO organization using CKAN API"""
        logger.info("Fetching HKO datasets using CKAN API...")
        
        # First, get organization details
        org_url = f"{self.api_url}/organization_show"
        org_params = {'id': self.hko_organization_id}
        
        try:
            response = self.session.get(org_url, params=org_params, timeout=30)
            response.raise_for_status()
            org_data = response.json()
            
            if org_data.get('success'):
                org_info = org_data['result']
                logger.info(f"Organization: {org_info.get('title', 'Unknown')}")
                logger.info(f"Description: {org_info.get('description', 'No description')}")
                
                # Get all datasets for this organization
                datasets_url = f"{self.api_url}/package_list"
                datasets_params = {'id': self.hko_organization_id}
                
                response = self.session.get(datasets_url, params=datasets_params, timeout=30)
                response.raise_for_status()
                datasets_list = response.json()
                
                if datasets_list.get('success'):
                    dataset_names = datasets_list['result']
                    logger.info(f"Found {len(dataset_names)} datasets in organization")
                    return dataset_names
                else:
                    logger.error(f"Failed to get dataset list: {datasets_list.get('error', 'Unknown error')}")
                    return []
            else:
                logger.error(f"Failed to get organization info: {org_data.get('error', 'Unknown error')}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return []
    
    def get_dataset_details(self, dataset_name):
        """Get detailed information for a specific dataset"""
        logger.info(f"Fetching details for dataset: {dataset_name}")
        
        dataset_url = f"{self.api_url}/package_show"
        params = {'id': dataset_name}
        
        try:
            response = self.session.get(dataset_url, params=params, timeout=30)
            response.raise_for_status()
            dataset_data = response.json()
            
            if dataset_data.get('success'):
                dataset = dataset_data['result']
                
                # Extract relevant information
                dataset_info = {
                    'name': dataset.get('name', ''),
                    'title': dataset.get('title', ''),
                    'description': dataset.get('notes', ''),
                    'url': f"{self.base_url}/en-dataset/{dataset_name}",
                    'organization': dataset.get('organization', {}).get('title', 'Hong Kong Observatory'),
                    'tags': [tag.get('name', '') for tag in dataset.get('tags', [])],
                    'license': dataset.get('license_title', ''),
                    'last_updated': dataset.get('metadata_modified', ''),
                    'created': dataset.get('metadata_created', ''),
                    'author': dataset.get('author', ''),
                    'maintainer': dataset.get('maintainer', ''),
                    'maintainer_email': dataset.get('maintainer_email', ''),
                    'resources': [],
                    'extras': dataset.get('extras', [])
                }
                
                # Extract resources (downloadable files)
                for resource in dataset.get('resources', []):
                    resource_info = {
                        'name': resource.get('name', ''),
                        'description': resource.get('description', ''),
                        'url': resource.get('url', ''),
                        'format': resource.get('format', '').upper(),
                        'size': resource.get('size', ''),
                        'last_modified': resource.get('last_modified', ''),
                        'created': resource.get('created', ''),
                        'mimetype': resource.get('mimetype', '')
                    }
                    dataset_info['resources'].append(resource_info)
                
                return dataset_info
            else:
                logger.error(f"Failed to get dataset details: {dataset_data.get('error', 'Unknown error')}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Failed to fetch dataset {dataset_name}: {e}")
            return None
    
    def scrape_all_datasets(self):
        """Main method to scrape all HKO datasets"""
        logger.info("Starting comprehensive HKO dataset scraping using CKAN API...")
        
        # Get list of all dataset names
        dataset_names = self.get_organization_datasets()
        
        if not dataset_names:
            logger.error("No datasets found for HKO organization")
            return []
        
        logger.info(f"Processing {len(dataset_names)} datasets...")
        
        # Scrape each dataset
        for i, dataset_name in enumerate(dataset_names, 1):
            logger.info(f"Processing dataset {i}/{len(dataset_names)}: {dataset_name}")
            
            dataset_info = self.get_dataset_details(dataset_name)
            if dataset_info:
                self.datasets.append(dataset_info)
                logger.info(f"Successfully scraped: {dataset_info['title']}")
            else:
                logger.warning(f"Failed to scrape dataset: {dataset_name}")
            
            # Be respectful - add delay between requests
            time.sleep(0.5)
        
        logger.info(f"Scraping completed. Found {len(self.datasets)} datasets.")
        return self.datasets
    
    def generate_report(self, output_format='both'):
        """Generate comprehensive report of all datasets"""
        logger.info("Generating comprehensive dataset report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format in ['json', 'both']:
            json_file = f"hko_datasets_detailed_report_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.datasets, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON report saved: {json_file}")
        
        if output_format in ['csv', 'both']:
            csv_file = f"hko_datasets_detailed_report_{timestamp}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                if self.datasets:
                    # Flatten the data for CSV
                    flattened_data = []
                    for dataset in self.datasets:
                        flat_dataset = {
                            'name': dataset['name'],
                            'title': dataset['title'],
                            'description': dataset['description'],
                            'url': dataset['url'],
                            'organization': dataset['organization'],
                            'tags': '; '.join(dataset['tags']),
                            'license': dataset['license'],
                            'last_updated': dataset['last_updated'],
                            'created': dataset['created'],
                            'author': dataset['author'],
                            'maintainer': dataset['maintainer'],
                            'maintainer_email': dataset['maintainer_email'],
                            'total_resources': len(dataset['resources']),
                            'resource_formats': '; '.join(set([r['format'] for r in dataset['resources'] if r['format']]))
                        }
                        flattened_data.append(flat_dataset)
                    
                    writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                    writer.writeheader()
                    writer.writerows(flattened_data)
            logger.info(f"CSV report saved: {csv_file}")
        
        # Generate markdown report
        md_file = f"hko_datasets_detailed_report_{timestamp}.md"
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
            f.write("# Hong Kong Observatory Datasets - Detailed Report\n\n")
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
                for resource in dataset['resources']:
                    if resource['format']:
                        all_formats.append(resource['format'])
            
            format_counts = {}
            for fmt in all_formats:
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
            
            if format_counts:
                f.write("## Data Formats Available\n\n")
                for fmt, count in sorted(format_counts.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"- **{fmt}**: {count} resources\n")
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
                
                if dataset['maintainer']:
                    f.write(f"**Maintainer:** {dataset['maintainer']}")
                    if dataset['maintainer_email']:
                        f.write(f" ({dataset['maintainer_email']})")
                    f.write("\n\n")
                
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
                        if resource['description']:
                            f.write(f"   - Description: {resource['description']}\n")
                        if resource['size']:
                            f.write(f"   - Size: {resource['size']}\n")
                        if resource['last_modified']:
                            f.write(f"   - Last Modified: {resource['last_modified']}\n")
                        f.write("\n")
                
                f.write("---\n\n")
    
    def run_full_scrape(self):
        """Run the complete scraping process"""
        logger.info("Starting full HKO dataset scraping process using CKAN API...")
        
        try:
            # Scrape all datasets
            datasets = self.scrape_all_datasets()
            
            if not datasets:
                logger.error("No datasets found. Please check the API endpoints.")
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
    scraper = ImprovedHKODatasetScraper()
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



