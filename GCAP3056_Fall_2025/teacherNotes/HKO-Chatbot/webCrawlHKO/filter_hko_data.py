#!/usr/bin/env python3
"""
Filter the comprehensive CSV report to extract only HKO-related data items
and create a focused report for Hong Kong Observatory datasets.
"""

import csv
import os
from datetime import datetime

def filter_hko_data(input_csv, output_csv):
    """Filter CSV to show only HKO-related data items"""
    
    hko_keywords = [
        'hko', 'observatory', 'weather', 'climate', 'meteorological',
        'temperature', 'rainfall', 'humidity', 'wind', 'typhoon',
        'forecast', 'warning', 'radar', 'satellite', 'lightning',
        'tide', 'radiation', 'air quality', 'atmospheric'
    ]
    
    hko_related_items = []
    
    print("Filtering HKO-related data items...")
    
    with open(input_csv, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            # Check if any field contains HKO-related keywords
            is_hko_related = False
            
            for field_name, field_value in row.items():
                if field_value and isinstance(field_value, str):
                    field_lower = field_value.lower()
                    for keyword in hko_keywords:
                        if keyword in field_lower:
                            is_hko_related = True
                            break
                if is_hko_related:
                    break
            
            if is_hko_related:
                hko_related_items.append(row)
    
    # Write filtered data to new CSV
    if hko_related_items:
        fieldnames = hko_related_items[0].keys()
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(hko_related_items)
        
        print(f"Found {len(hko_related_items)} HKO-related items")
        print(f"Filtered report saved to: {output_csv}")
    else:
        print("No HKO-related items found in the data")
    
    return hko_related_items

def create_hko_focused_report():
    """Create a focused report specifically for HKO data access"""
    
    # Key HKO-related endpoints and information
    hko_focused_data = [
        {
            'type': 'hko_provider_page',
            'description': 'Direct HKO provider page on data.gov.hk',
            'url': '/en-datasets/provider/hk-hko',
            'full_url': 'https://data.gov.hk/en-datasets/provider/hk-hko',
            'dataset_count': '52',
            'source': 'data.gov.hk'
        },
        {
            'type': 'climate_weather_category',
            'description': 'Climate and Weather category containing HKO datasets',
            'url': '/en-datasets/category/climate-and-weather',
            'full_url': 'https://data.gov.hk/en-datasets/category/climate-and-weather',
            'dataset_count': 'Multiple',
            'source': 'data.gov.hk'
        },
        {
            'type': 'hko_api_endpoint',
            'description': 'Main dataset API endpoint (requires authentication)',
            'url': '/api/v1/datasets',
            'full_url': 'https://data.gov.hk/api/v1/datasets',
            'dataset_count': 'All datasets',
            'source': 'data.gov.hk'
        },
        {
            'type': 'providers_json',
            'description': 'JSON data containing HKO provider information',
            'url': '/filestore/json/providers_en.json',
            'full_url': 'https://data.gov.hk/filestore/json/providers_en.json',
            'dataset_count': '52 (confirmed)',
            'source': 'data.gov.hk'
        },
        {
            'type': 'categories_json',
            'description': 'JSON data for climate and weather categories',
            'url': '/filestore/json/categories_en.json',
            'full_url': 'https://data.gov.hk/filestore/json/categories_en.json',
            'dataset_count': 'Category data',
            'source': 'data.gov.hk'
        },
        {
            'type': 'rss_feed',
            'description': 'RSS feed for dataset updates including HKO data',
            'url': '/filestore/feeds/data_rss_en.xml',
            'full_url': 'https://data.gov.hk/filestore/feeds/data_rss_en.xml',
            'dataset_count': 'Recent updates',
            'source': 'data.gov.hk'
        },
        {
            'type': 'hko_website',
            'description': 'Official Hong Kong Observatory website',
            'url': '/',
            'full_url': 'https://www.hko.gov.hk/',
            'dataset_count': 'Various data sources',
            'source': 'hko.gov.hk'
        },
        {
            'type': 'contact_info',
            'description': 'Contact information for data access inquiries',
            'url': 'mailto:enquiry@1835500.gov.hk',
            'full_url': 'mailto:enquiry@1835500.gov.hk',
            'dataset_count': 'Support',
            'source': 'data.gov.hk'
        }
    ]
    
    # Generate timestamp for output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/hko_focused_report_{timestamp}.csv"
    
    # Write focused report
    fieldnames = ['type', 'description', 'url', 'full_url', 'dataset_count', 'source']
    
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hko_focused_data)
    
    print(f"HKO-focused report created: {output_file}")
    return output_file

def main():
    """Main function to filter and create HKO-focused reports"""
    print("=== Creating HKO-Focused Data Report ===")
    
    # Input file
    input_csv = "/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/data_gov_hk_comprehensive_report_20251024_154241.csv"
    
    # Generate timestamp for output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filtered_csv = f"/Users/simonwang/Documents/Usage/GCAP3056/gcap3056/GCAP3056_Fall_2025/HKO-Chatbot/webCrawlHKO/hko_filtered_data_{timestamp}.csv"
    
    # Filter HKO-related data
    hko_items = filter_hko_data(input_csv, filtered_csv)
    
    # Create focused HKO report
    focused_report = create_hko_focused_report()
    
    print("\n=== HKO DATA ACCESS SUMMARY ===")
    print("Key locations to find HKO datasets:")
    print("1. Direct HKO Provider Page: https://data.gov.hk/en-datasets/provider/hk-hko")
    print("2. Climate and Weather Category: https://data.gov.hk/en-datasets/category/climate-and-weather")
    print("3. RSS Feed: https://data.gov.hk/filestore/feeds/data_rss_en.xml")
    print("4. Providers JSON: https://data.gov.hk/filestore/json/providers_en.json")
    print("5. Contact: enquiry@1835500.gov.hk")
    
    print(f"\nReports generated:")
    print(f"- Filtered HKO data: {filtered_csv}")
    print(f"- HKO-focused report: {focused_report}")

if __name__ == "__main__":
    main()


