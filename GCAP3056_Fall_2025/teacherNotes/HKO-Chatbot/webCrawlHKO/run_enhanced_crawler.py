#!/usr/bin/env python3
"""
Enhanced HKO Web Crawler Runner
Runs the advanced crawler with content analysis for Dr Tin chatbot mentions
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

from enhanced_hko_crawler import EnhancedHKOWebCrawler

def main():
    print("Starting Enhanced HKO Web Crawler...")
    print("Performing advanced content analysis for Dr Tin chatbot mentions")
    print("=" * 60)
    
    # Initialize enhanced crawler
    crawler = EnhancedHKOWebCrawler()
    
    # Run the enhanced crawler
    try:
        crawler.crawl(max_pages=500, delay=1)
        print("\nEnhanced crawling completed successfully!")
        print(f"Check the output directory for downloaded files:")
        print(f"  {crawler.output_dir}")
        print(f"Check the detailed logs:")
        print(f"  {crawler.output_dir / 'logs' / 'enhanced_hko_crawler.log'}")
        print(f"Check the main log file:")
        print(f"  {crawler.output_dir / 'webCrawllog_HKO.md'}")
        print(f"Check the comprehensive reports:")
        print(f"  {crawler.output_dir / 'reports'}")
        
        # Show summary statistics
        dr_tin_count = len([p for p in crawler.site_map if p.get('has_dr_tin_mention', False)])
        high_relevance_count = len([p for p in crawler.site_map if p.get('relevance_score', 0) > 0.5])
        
        print(f"\nSummary:")
        print(f"  - Total pages crawled: {len(crawler.visited_urls)}")
        print(f"  - Dr Tin chatbot mentions found: {dr_tin_count}")
        print(f"  - High relevance pages: {high_relevance_count}")
        print(f"  - Failed downloads: {len(crawler.failed_urls)}")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
        print("Partial results have been saved.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


