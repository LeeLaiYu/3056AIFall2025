#!/usr/bin/env python3
"""
Comprehensive Crawler for CyberDefender.hk
Handles duplicates and creates complete site map
"""

from web_crawler import CyberDefenderCrawler
import time

class ComprehensiveCyberDefenderCrawler(CyberDefenderCrawler):
    def __init__(self):
        super().__init__()
        
    def run_comprehensive_crawl(self, max_pages=1000, delay=1):
        """Run a comprehensive crawl with duplicate handling"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING COMPREHENSIVE CYBERDEFENDER CRAWL")
        self.logger.info("=" * 60)
        
        # Phase 1: Initial crawl to discover all links
        self.logger.info("Phase 1: Initial discovery crawl...")
        self.crawl(max_pages=200, delay=delay)
        
        # Phase 2: Crawl all discovered unique URLs
        self.logger.info(f"Phase 2: Crawling {len(self.unique_urls_to_crawl)} unique discovered URLs...")
        self.crawl_all_discovered(max_pages=max_pages, delay=delay)
        
        # Generate final reports
        self.generate_comprehensive_report()
        
        self.logger.info("=" * 60)
        self.logger.info("COMPREHENSIVE CRAWL COMPLETED")
        self.logger.info("=" * 60)
        
    def generate_comprehensive_report(self):
        """Generate comprehensive report with statistics"""
        report_file = self.output_dir / "comprehensive_crawl_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# Comprehensive CyberDefender Crawl Report\n\n")
            f.write(f"**Crawl Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Base URL:** {self.base_url}\n\n")
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Total Pages Crawled:** {len(self.visited_urls)}\n")
            f.write(f"- **Unique URLs Discovered:** {len(self.unique_urls_to_crawl)}\n")
            f.write(f"- **Total Links Found:** {sum(page['links_found'] for page in self.site_map)}\n")
            f.write(f"- **Successful Downloads:** {len([p for p in self.site_map if p['status'] == 'success'])}\n")
            f.write(f"- **Failed Downloads:** {len([p for p in self.site_map if p['status'] == 'failed'])}\n\n")
            
            # Category breakdown
            categories = {}
            for page in self.site_map:
                cat = self.categorize_url(page['url'])
                categories[cat] = categories.get(cat, 0) + 1
            
            f.write("## Content Categories\n\n")
            for category, count in sorted(categories.items()):
                f.write(f"- **{category}:** {count} pages\n")
            
            f.write("\n## Duplicate Handling\n\n")
            f.write(f"- **URLs Normalized:** {len(self.url_normalization_cache)}\n")
            f.write(f"- **Duplicate URLs Removed:** {len(self.all_discovered_urls) - len(self.unique_urls_to_crawl)}\n")
            
            f.write("\n## Files Generated\n\n")
            f.write("- `sitemap.csv` - Complete site map in CSV format\n")
            f.write("- `all_discovered_urls.txt` - All discovered URLs\n")
            f.write("- `crawl_summary.md` - Detailed crawl summary\n")
            f.write("- `comprehensive_crawl_report.md` - This report\n")
            f.write("- `crawler.log` - Detailed logs\n")
        
        self.logger.info(f"Comprehensive report saved to: {report_file}")

def main():
    """Main function to run the comprehensive crawler"""
    print("Starting Comprehensive CyberDefender Crawler...")
    print("This will handle duplicates and create a complete site map")
    print("=" * 60)
    
    crawler = ComprehensiveCyberDefenderCrawler()
    
    try:
        crawler.run_comprehensive_crawl(max_pages=1000, delay=1)
        
        print("\n" + "=" * 60)
        print("COMPREHENSIVE CRAWL COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Total pages crawled: {len(crawler.visited_urls)}")
        print(f"Unique URLs discovered: {len(crawler.unique_urls_to_crawl)}")
        print(f"Files saved to: {crawler.output_dir}")
        print("\nGenerated files:")
        print(f"  - sitemap.csv (complete site map)")
        print(f"  - all_discovered_urls.txt (all URLs)")
        print(f"  - comprehensive_crawl_report.md (detailed report)")
        print(f"  - crawl_summary.md (summary)")
        print(f"  - crawler.log (detailed logs)")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
        print("Partial results have been saved.")
    except Exception as e:
        print(f"\nError during crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


