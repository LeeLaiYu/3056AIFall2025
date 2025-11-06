#!/usr/bin/env python3
"""
Process all 5 HTML pages from data.gov.hk and generate a comprehensive CSV report
listing all available data items, navigation links, API endpoints, and metadata.
"""

import os
import re
import json
import csv
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def extract_navigation_links(soup):
    """Extract navigation links and categories from the HTML"""
    navigation_data = []
    
    # Extract main navigation menu items
    menu_items = soup.find_all('a', class_='menu__link')
    for item in menu_items:
        href = item.get('href', '')
        text = item.get_text(strip=True)
        if href and text:
            navigation_data.append({
                'type': 'navigation_link',
                'text': text,
                'url': href,
                'full_url': urljoin('https://data.gov.hk', href)
            })
    
    # Extract category links
    category_links = soup.find_all('a', href=re.compile(r'/en-datasets/category/'))
    for link in category_links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        if href and text:
            navigation_data.append({
                'type': 'category_link',
                'text': text,
                'url': href,
                'full_url': urljoin('https://data.gov.hk', href)
            })
    
    return navigation_data

def extract_api_endpoints(soup):
    """Extract API endpoints and JSON data sources from the HTML"""
    api_data = []
    
    # Look for data-url attributes in select elements
    select_elements = soup.find_all('select', {'data-url': True})
    for select in select_elements:
        data_url = select.get('data-url', '')
        if data_url:
            api_data.append({
                'type': 'json_endpoint',
                'url': data_url,
                'full_url': urljoin('https://data.gov.hk', data_url),
                'description': f"JSON data for {select.get('id', 'unknown')} dropdown"
            })
    
    # Look for dataset listing API
    dataset_listing = soup.find('div', {'data-url': True, 'id': 'dataset-listing'})
    if dataset_listing:
        api_url = dataset_listing.get('data-url', '')
        if api_url:
            api_data.append({
                'type': 'dataset_api',
                'url': api_url,
                'full_url': urljoin('https://data.gov.hk', api_url),
                'description': 'Main dataset listing API endpoint'
            })
    
    # Look for RSS feed
    rss_links = soup.find_all('a', href=re.compile(r'\.xml$'))
    for link in rss_links:
        href = link.get('href', '')
        if 'rss' in href.lower() or 'feed' in href.lower():
            api_data.append({
                'type': 'rss_feed',
                'url': href,
                'full_url': urljoin('https://data.gov.hk', href),
                'description': 'RSS feed for dataset updates'
            })
    
    return api_data

def extract_search_functionality(soup):
    """Extract search functionality details"""
    search_data = []
    
    # Extract search form elements
    search_forms = soup.find_all('form', class_=re.compile(r'search'))
    for form in search_forms:
        form_id = form.get('id', 'unknown')
        search_inputs = form.find_all('input', {'type': 'search'})
        for input_elem in search_inputs:
            placeholder = input_elem.get('placeholder', '')
            input_id = input_elem.get('id', '')
            search_data.append({
                'type': 'search_input',
                'form_id': form_id,
                'input_id': input_id,
                'placeholder': placeholder,
                'description': f"Search input in {form_id}"
            })
    
    # Extract filter options
    filter_selects = soup.find_all('select', {'name': re.compile(r'\[\]$')})
    for select in filter_selects:
        name = select.get('name', '')
        select_id = select.get('id', '')
        data_url = select.get('data-url', '')
        search_data.append({
            'type': 'filter_select',
            'name': name,
            'select_id': select_id,
            'data_url': data_url,
            'description': f"Filter dropdown: {name}"
        })
    
    return search_data

def extract_metadata(soup):
    """Extract page metadata"""
    metadata = []
    
    # Extract title
    title = soup.find('title')
    if title:
        metadata.append({
            'type': 'page_title',
            'value': title.get_text(strip=True),
            'description': 'Page title'
        })
    
    # Extract meta description
    meta_desc = soup.find('meta', {'name': 'description'})
    if meta_desc:
        metadata.append({
            'type': 'meta_description',
            'value': meta_desc.get('content', ''),
            'description': 'Page meta description'
        })
    
    # Extract breadcrumb
    breadcrumbs = soup.find_all('a', class_='breadcrumb__link')
    for i, breadcrumb in enumerate(breadcrumbs):
        metadata.append({
            'type': 'breadcrumb',
            'value': breadcrumb.get_text(strip=True),
            'description': f'Breadcrumb item {i+1}'
        })
    
    # Extract page heading
    page_title = soup.find('h1', class_='page-title')
    if page_title:
        metadata.append({
            'type': 'page_heading',
            'value': page_title.get_text(strip=True),
            'description': 'Main page heading'
        })
    
    return metadata

def extract_contact_info(soup):
    """Extract contact information"""
    contact_data = []
    
    # Extract email addresses
    email_links = soup.find_all('a', href=re.compile(r'^mailto:'))
    for link in email_links:
        email = link.get('href', '').replace('mailto:', '')
        contact_data.append({
            'type': 'email',
            'value': email,
            'description': 'Contact email'
        })
    
    # Extract phone numbers
    phone_elements = soup.find_all(text=re.compile(r'\d{3}\s?\d{4}'))
    for phone in phone_elements:
        contact_data.append({
            'type': 'phone',
            'value': phone.strip(),
            'description': 'Contact phone number'
        })
    
    return contact_data

def process_html_file(file_path):
    """Process a single HTML file and extract all data"""
    print(f"Processing {file_path}...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract all types of data
        navigation_links = extract_navigation_links(soup)
        api_endpoints = extract_api_endpoints(soup)
        search_functionality = extract_search_functionality(soup)
        metadata = extract_metadata(soup)
        contact_info = extract_contact_info(soup)
        
        # Combine all data
        all_data = []
        all_data.extend(navigation_links)
        all_data.extend(api_endpoints)
        all_data.extend(search_functionality)
        all_data.extend(metadata)
        all_data.extend(contact_info)
        
        # Add page identifier to each item
        page_name = os.path.basename(file_path)
        for item in all_data:
            item['source_page'] = page_name
        
        return all_data
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []

def generate_csv_report(all_data, output_file):
    """Generate CSV report from all extracted data"""
    if not all_data:
        print("No data to write to CSV")
        return
    
    # Get all unique field names
    fieldnames = set()
    for item in all_data:
        fieldnames.update(item.keys())
    fieldnames = sorted(list(fieldnames))
    
    # Write CSV file
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_data)
    
    print(f"CSV report generated: {output_file}")
    print(f"Total items extracted: {len(all_data)}")

def main():
    """Main function to process all HTML pages and generate report"""
    print("=== Processing All HTML Pages from data.gov.hk ===")
    
    # Define the directory containing HTML files
    html_dir = "/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/data.gov.hk"
    
    # Get all HTML files
    html_files = []
    for i in range(1, 6):  # pages 1-5
        file_path = os.path.join(html_dir, f"page{i}.html")
        if os.path.exists(file_path):
            html_files.append(file_path)
    
    print(f"Found {len(html_files)} HTML files to process")
    
    # Process all files
    all_extracted_data = []
    for file_path in html_files:
        data = process_html_file(file_path)
        all_extracted_data.extend(data)
    
    # Generate timestamp for output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/data_gov_hk_comprehensive_report_{timestamp}.csv"
    
    # Generate CSV report
    generate_csv_report(all_extracted_data, output_file)
    
    # Print summary statistics
    print("\n=== EXTRACTION SUMMARY ===")
    print(f"Total items extracted: {len(all_extracted_data)}")
    
    # Count by type
    type_counts = {}
    for item in all_extracted_data:
        item_type = item.get('type', 'unknown')
        type_counts[item_type] = type_counts.get(item_type, 0) + 1
    
    print("\nItems by type:")
    for item_type, count in sorted(type_counts.items()):
        print(f"  {item_type}: {count}")
    
    # Count by source page
    page_counts = {}
    for item in all_extracted_data:
        page = item.get('source_page', 'unknown')
        page_counts[page] = page_counts.get(page, 0) + 1
    
    print("\nItems by source page:")
    for page, count in sorted(page_counts.items()):
        print(f"  {page}: {count}")
    
    print(f"\nDetailed report saved to: {output_file}")

if __name__ == "__main__":
    main()


