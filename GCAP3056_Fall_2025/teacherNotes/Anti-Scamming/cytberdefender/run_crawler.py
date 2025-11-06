#!/usr/bin/env python3
"""
Simple script to run the CyberDefender crawler
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

from web_crawler import CyberDefenderCrawler

def main():
    print("Starting CyberDefender Web Crawler...")
    print("=" * 50)
    
    # Initialize crawler
    crawler = CyberDefenderCrawler()
    
    # Run the crawler
    try:
        crawler.crawl(max_pages=200, delay=1)
        print("\nCrawling completed successfully!")
        print(f"Check the output directory for downloaded files:")
        print(f"  {crawler.output_dir}")
        print(f"Check the log file for detailed information:")
        print(f"  {crawler.output_dir / 'crawler.log'}")
        print(f"Check the summary file:")
        print(f"  {crawler.output_dir / 'crawl_summary.md'}")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


