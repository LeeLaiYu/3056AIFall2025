#!/usr/bin/env python3
"""
Simple script to run the HKO web crawler
Searches for 'Dr Tin chatbot' mentions on the HKO website
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

from hko_web_crawler import HKOWebCrawler

def main():
    print("Starting HKO Web Crawler...")
    print("Searching for 'Dr Tin chatbot' mentions on HKO website")
    print("=" * 60)
    
    # Initialize crawler
    crawler = HKOWebCrawler()
    
    # Run the crawler
    try:
        crawler.crawl(max_pages=500, delay=1)
        print("\nCrawling completed successfully!")
        print(f"Check the output directory for downloaded files:")
        print(f"  {crawler.output_dir}")
        print(f"Check the log file for detailed information:")
        print(f"  {crawler.output_dir / 'hko_crawler.log'}")
        print(f"Check the main log file:")
        print(f"  {crawler.output_dir / 'webCrawllog_HKO.md'}")
        print(f"Check the reports directory:")
        print(f"  {crawler.reports_dir}")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


