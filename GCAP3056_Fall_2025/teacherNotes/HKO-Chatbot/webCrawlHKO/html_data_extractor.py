#!/usr/bin/env python3
"""
HTML Data Extractor
Extracts and analyzes data from the data.gov.hk HTML file
"""

import json
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HTMLDataExtractor:
    def __init__(self, html_file_path):
        self.html_file_path = html_file_path
        self.base_url = "https://data.gov.hk"
        self.extracted_data = {}
        
    def load_html(self):
        """Load and parse the HTML file"""
        try:
            with open(self.html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            self.soup = BeautifulSoup(html_content, 'html.parser')
            logger.info("HTML file loaded and parsed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to load HTML file: {e}")
            return False
    
    def extract_page_metadata(self):
        """Extract page metadata"""
        metadata = {
            'title': '',
            'description': '',
            'url': '',
            'language': '',
            'viewport': '',
            'charset': ''
        }
        
        # Extract title
        title_tag = self.soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text().strip()
        
        # Extract description
        desc_meta = self.soup.find('meta', attrs={'name': 'description'})
        if desc_meta:
            metadata['description'] = desc_meta.get('content', '').strip()
        
        # Extract language
        html_tag = self.soup.find('html')
        if html_tag:
            metadata['language'] = html_tag.get('lang', '')
        
        # Extract charset
        charset_meta = self.soup.find('meta', attrs={'charset': True})
        if charset_meta:
            metadata['charset'] = charset_meta.get('charset', '')
        
        # Extract viewport
        viewport_meta = self.soup.find('meta', attrs={'name': 'viewport'})
        if viewport_meta:
            metadata['viewport'] = viewport_meta.get('content', '')
        
        self.extracted_data['metadata'] = metadata
        logger.info("Page metadata extracted")
    
    def extract_navigation_links(self):
        """Extract navigation and menu links"""
        nav_links = {
            'main_menu': [],
            'category_links': [],
            'provider_links': [],
            'help_links': [],
            'community_links': []
        }
        
        # Extract main menu links
        main_menu = self.soup.find('nav', class_='menu')
        if main_menu:
            menu_items = main_menu.find_all('a', class_='menu__link')
            for item in menu_items:
                link_data = {
                    'text': item.get_text().strip(),
                    'href': item.get('href', ''),
                    'full_url': urljoin(self.base_url, item.get('href', ''))
                }
                nav_links['main_menu'].append(link_data)
        
        # Extract category links
        category_links = self.soup.find_all('a', href=re.compile(r'/en-datasets/category/'))
        for link in category_links:
            link_data = {
                'text': link.get_text().strip(),
                'href': link.get('href', ''),
                'full_url': urljoin(self.base_url, link.get('href', ''))
            }
            nav_links['category_links'].append(link_data)
        
        # Extract provider links
        provider_links = self.soup.find_all('a', href=re.compile(r'/en/providers'))
        for link in provider_links:
            link_data = {
                'text': link.get_text().strip(),
                'href': link.get('href', ''),
                'full_url': urljoin(self.base_url, link.get('href', ''))
            }
            nav_links['provider_links'].append(link_data)
        
        self.extracted_data['navigation'] = nav_links
        logger.info(f"Navigation links extracted: {len(nav_links['main_menu'])} main menu, {len(nav_links['category_links'])} categories")
    
    def extract_search_functionality(self):
        """Extract search functionality information"""
        search_info = {
            'search_form': {},
            'filters': {},
            'sorting_options': [],
            'api_endpoints': []
        }
        
        # Extract search form
        search_form = self.soup.find('form', id='form-dataset-search')
        if search_form:
            search_info['search_form'] = {
                'id': search_form.get('id', ''),
                'action': search_form.get('action', ''),
                'method': search_form.get('method', '')
            }
        
        # Extract filter options
        filter_selects = self.soup.find_all('select', class_='dataset-search__select')
        for select in filter_selects:
            select_id = select.get('id', '')
            select_name = select.get('name', '')
            data_url = select.get('data-url', '')
            
            if select_id:
                search_info['filters'][select_id] = {
                    'name': select_name,
                    'data_url': data_url,
                    'options': []
                }
                
                # Extract options
                options = select.find_all('option')
                for option in options:
                    option_data = {
                        'value': option.get('value', ''),
                        'text': option.get_text().strip(),
                        'selected': option.has_attr('selected')
                    }
                    search_info['filters'][select_id]['options'].append(option_data)
        
        # Extract sorting options
        sort_select = self.soup.find('select', id='dataset-search-sort')
        if sort_select:
            sort_options = sort_select.find_all('option')
            for option in sort_options:
                sort_data = {
                    'value': option.get('value', ''),
                    'text': option.get_text().strip(),
                    'selected': option.has_attr('selected')
                }
                search_info['sorting_options'].append(sort_data)
        
        # Extract API endpoints
        api_links = self.soup.find_all('a', href=re.compile(r'/api/'))
        for link in api_links:
            api_data = {
                'text': link.get_text().strip(),
                'href': link.get('href', ''),
                'full_url': urljoin(self.base_url, link.get('href', ''))
            }
            search_info['api_endpoints'].append(api_data)
        
        self.extracted_data['search_functionality'] = search_info
        logger.info("Search functionality extracted")
    
    def extract_dataset_listing_info(self):
        """Extract dataset listing information"""
        listing_info = {
            'total_results': 0,
            'current_page': 1,
            'api_endpoint': '',
            'templates': [],
            'pagination': {}
        }
        
        # Extract total results
        total_span = self.soup.find('span', class_='dataset-listing__total-num')
        if total_span:
            try:
                listing_info['total_results'] = int(total_span.get_text().strip())
            except ValueError:
                listing_info['total_results'] = 0
        
        # Extract API endpoint
        dataset_listing = self.soup.find('div', id='dataset-listing')
        if dataset_listing:
            data_url = dataset_listing.get('data-url', '')
            if data_url:
                listing_info['api_endpoint'] = urljoin(self.base_url, data_url)
        
        # Extract templates
        templates = self.soup.find_all('template')
        for template in templates:
            template_id = template.get('id', '')
            if template_id:
                listing_info['templates'].append({
                    'id': template_id,
                    'content': template.get_text().strip()[:200] + '...' if len(template.get_text().strip()) > 200 else template.get_text().strip()
                })
        
        # Extract pagination info
        pagination = self.soup.find('div', class_='dataset-listing__pagination')
        if pagination:
            range_span = pagination.find('span', class_='dataset-listing__range')
            if range_span:
                listing_info['pagination']['current_range'] = range_span.get_text().strip()
            
            total_span = pagination.find('span', class_='dataset-listing__total-num')
            if total_span:
                listing_info['pagination']['total'] = total_span.get_text().strip()
        
        self.extracted_data['dataset_listing'] = listing_info
        logger.info(f"Dataset listing info extracted: {listing_info['total_results']} results")
    
    def extract_rss_feed_info(self):
        """Extract RSS feed information"""
        rss_info = {
            'rss_url': '',
            'description': '',
            'about': ''
        }
        
        # Extract RSS URL
        rss_link = self.soup.find('a', href=re.compile(r'data_rss_en\.xml'))
        if rss_link:
            rss_info['rss_url'] = rss_link.get('href', '')
        
        # Extract RSS description
        rss_desc = self.soup.find('p', string=re.compile(r'This daily updated RSS feed'))
        if rss_desc:
            rss_info['description'] = rss_desc.get_text().strip()
        
        # Extract RSS about section
        rss_about = self.soup.find('p', string=re.compile(r'Really Simple Syndication'))
        if rss_about:
            rss_info['about'] = rss_about.get_text().strip()
        
        self.extracted_data['rss_feed'] = rss_info
        logger.info("RSS feed information extracted")
    
    def extract_contact_info(self):
        """Extract contact information"""
        contact_info = {
            'email': '',
            'phone': '',
            'organization': '',
            'links': []
        }
        
        # Extract email
        email_link = self.soup.find('a', href=re.compile(r'mailto:'))
        if email_link:
            contact_info['email'] = email_link.get('href', '').replace('mailto:', '')
        
        # Extract phone
        phone_span = self.soup.find('span', string=re.compile(r'183 5500'))
        if phone_span:
            contact_info['phone'] = phone_span.get_text().strip()
        
        # Extract organization
        org_span = self.soup.find('p', string=re.compile(r'Developed and Supported by'))
        if org_span:
            contact_info['organization'] = org_span.get_text().strip()
        
        # Extract footer links
        footer_links = self.soup.find('div', class_='foot-row').find_all('a')
        for link in footer_links:
            link_data = {
                'text': link.get_text().strip(),
                'href': link.get('href', ''),
                'full_url': urljoin(self.base_url, link.get('href', ''))
            }
            contact_info['links'].append(link_data)
        
        self.extracted_data['contact_info'] = contact_info
        logger.info("Contact information extracted")
    
    def generate_report(self):
        """Generate comprehensive report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate JSON report
        json_file = f"html_extraction_report_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.extracted_data, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON report saved: {json_file}")
        
        # Generate CSV report for navigation links
        csv_file = f"html_extraction_report_{timestamp}.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Type', 'Text', 'URL', 'Full URL'])
            
            # Write navigation links
            for link in self.extracted_data.get('navigation', {}).get('main_menu', []):
                writer.writerow(['Main Menu', link['text'], link['href'], link['full_url']])
            
            for link in self.extracted_data.get('navigation', {}).get('category_links', []):
                writer.writerow(['Category', link['text'], link['href'], link['full_url']])
            
            for link in self.extracted_data.get('navigation', {}).get('provider_links', []):
                writer.writerow(['Provider', link['text'], link['href'], link['full_url']])
        
        logger.info(f"CSV report saved: {csv_file}")
        
        # Generate markdown report
        md_file = f"html_extraction_report_{timestamp}.md"
        self.generate_markdown_report(md_file)
        logger.info(f"Markdown report saved: {md_file}")
        
        return {
            'json_file': json_file,
            'csv_file': csv_file,
            'markdown_file': md_file
        }
    
    def generate_markdown_report(self, filename):
        """Generate markdown report"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("# Data.gov.hk HTML Analysis Report\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Source:** {self.html_file_path}\n\n")
            
            # Page metadata
            metadata = self.extracted_data.get('metadata', {})
            f.write("## Page Metadata\n\n")
            f.write(f"- **Title:** {metadata.get('title', 'N/A')}\n")
            f.write(f"- **Description:** {metadata.get('description', 'N/A')}\n")
            f.write(f"- **Language:** {metadata.get('language', 'N/A')}\n")
            f.write(f"- **Charset:** {metadata.get('charset', 'N/A')}\n\n")
            
            # Navigation links
            nav = self.extracted_data.get('navigation', {})
            f.write("## Navigation Links\n\n")
            f.write(f"### Main Menu ({len(nav.get('main_menu', []))})\n")
            for link in nav.get('main_menu', []):
                f.write(f"- [{link['text']}]({link['full_url']})\n")
            
            f.write(f"\n### Category Links ({len(nav.get('category_links', []))})\n")
            for link in nav.get('category_links', []):
                f.write(f"- [{link['text']}]({link['full_url']})\n")
            
            # Search functionality
            search = self.extracted_data.get('search_functionality', {})
            f.write("\n## Search Functionality\n\n")
            f.write(f"### Filters ({len(search.get('filters', {}))})\n")
            for filter_id, filter_data in search.get('filters', {}).items():
                f.write(f"- **{filter_id}**: {filter_data.get('name', 'N/A')}\n")
                f.write(f"  - Data URL: {filter_data.get('data_url', 'N/A')}\n")
                f.write(f"  - Options: {len(filter_data.get('options', []))}\n")
            
            f.write(f"\n### Sorting Options ({len(search.get('sorting_options', []))})\n")
            for option in search.get('sorting_options', []):
                f.write(f"- {option['text']} ({option['value']})\n")
            
            # Dataset listing
            listing = self.extracted_data.get('dataset_listing', {})
            f.write("\n## Dataset Listing Information\n\n")
            f.write(f"- **Total Results:** {listing.get('total_results', 0)}\n")
            f.write(f"- **API Endpoint:** {listing.get('api_endpoint', 'N/A')}\n")
            f.write(f"- **Templates:** {len(listing.get('templates', []))}\n")
            
            # RSS feed
            rss = self.extracted_data.get('rss_feed', {})
            f.write("\n## RSS Feed Information\n\n")
            f.write(f"- **RSS URL:** {rss.get('rss_url', 'N/A')}\n")
            f.write(f"- **Description:** {rss.get('description', 'N/A')}\n")
            
            # Contact info
            contact = self.extracted_data.get('contact_info', {})
            f.write("\n## Contact Information\n\n")
            f.write(f"- **Email:** {contact.get('email', 'N/A')}\n")
            f.write(f"- **Phone:** {contact.get('phone', 'N/A')}\n")
            f.write(f"- **Organization:** {contact.get('organization', 'N/A')}\n")
            
            f.write("\n## Key Findings\n\n")
            f.write("1. **No Datasets Found**: The page shows 'Total 0 results' indicating no datasets are currently available\n")
            f.write("2. **JavaScript-Dependent**: The page relies heavily on JavaScript for content loading\n")
            f.write("3. **API Endpoint**: The page references `/api/v1/datasets` for data loading\n")
            f.write("4. **RSS Feed Available**: There's an RSS feed for dataset updates\n")
            f.write("5. **Multiple Categories**: The site supports various data categories including Climate and Weather\n")
    
    def run_extraction(self):
        """Run the complete extraction process"""
        logger.info("Starting HTML data extraction...")
        
        if not self.load_html():
            return None
        
        try:
            # Extract all data
            self.extract_page_metadata()
            self.extract_navigation_links()
            self.extract_search_functionality()
            self.extract_dataset_listing_info()
            self.extract_rss_feed_info()
            self.extract_contact_info()
            
            # Generate reports
            report_files = self.generate_report()
            
            logger.info("HTML data extraction completed successfully!")
            logger.info(f"Report files generated: {report_files}")
            
            return {
                'extracted_data': self.extracted_data,
                'report_files': report_files
            }
            
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            return None

def main():
    """Main function to run the extractor"""
    html_file = "/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/data.gov.hk/page1.html"
    
    extractor = HTMLDataExtractor(html_file)
    result = extractor.run_extraction()
    
    if result:
        print(f"\n✅ HTML data extraction completed!")
        print(f"📊 Extracted data from HTML file")
        print(f"📁 Report files generated:")
        for file_type, file_path in result['report_files'].items():
            print(f"   - {file_type}: {file_path}")
    else:
        print("❌ HTML data extraction failed. Check the logs for details.")

if __name__ == "__main__":
    main()


