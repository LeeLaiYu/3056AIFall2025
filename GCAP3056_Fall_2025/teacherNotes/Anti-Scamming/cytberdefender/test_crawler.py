#!/usr/bin/env python3
"""
Test script for the CyberDefender crawler
Runs a limited crawl to test functionality
"""

from web_crawler import CyberDefenderCrawler

def test_crawler():
    print("Testing CyberDefender Crawler...")
    print("=" * 40)
    
    # Initialize crawler
    crawler = CyberDefenderCrawler()
    
    # Run a limited test crawl
    print("Running test crawl (max 5 pages)...")
    crawler.crawl(max_pages=5, delay=2)
    
    print("\nTest completed!")
    print(f"Downloaded files: {len(crawler.downloaded_files)}")
    print(f"Failed downloads: {len(crawler.failed_urls)}")
    
    if crawler.downloaded_files:
        print("\nDownloaded files:")
        for file_info in crawler.downloaded_files:
            print(f"  - {file_info['filename']}")

if __name__ == "__main__":
    test_crawler()


