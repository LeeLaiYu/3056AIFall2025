#!/usr/bin/env python3
"""
Deep Crawler for CyberDefender.hk
Crawls all discovered links and creates comprehensive site map
"""

from web_crawler import CyberDefenderCrawler
import time

class DeepCyberDefenderCrawler(CyberDefenderCrawler):
    def __init__(self):
        super().__init__()
        self.max_depth = 3  # Maximum depth to crawl
        self.current_depth = 0
        
    def crawl_deep(self, max_pages=500, delay=1):
        """Deep crawl with higher page limit"""
        self.logger.info(f"Starting DEEP crawl of {self.base_url}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Max pages: {max_pages}")
        
        urls_to_visit = [self.base_url]
        pages_crawled = 0
        
        while urls_to_visit and pages_crawled < max_pages:
            current_url = urls_to_visit.pop(0)
            
            if current_url in self.visited_urls:
                continue
                
            self.visited_urls.add(current_url)
            
            # Download the page
            new_links = self.download_page(current_url)
            pages_crawled += 1
            
            # Add new links to the queue (prioritize same domain)
            for link in new_links:
                if link not in self.visited_urls and link not in urls_to_visit:
                    # Prioritize cyberdefender.hk links
                    if 'cyberdefender.hk' in link:
                        urls_to_visit.insert(0, link)  # Add to front
                    else:
                        urls_to_visit.append(link)    # Add to back
            
            # Respectful crawling delay
            time.sleep(delay)
            
            self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(urls_to_visit)} URLs in queue")
            
            # Log discovered URLs count every 50 pages
            if pages_crawled % 50 == 0:
                self.logger.info(f"Total unique URLs discovered so far: {len(self.all_discovered_urls)}")
        
        self.generate_summary()
        self.generate_sitemap_csv()
        self.logger.info("Deep crawling completed!")

def main():
    """Main function to run the deep crawler"""
    print("Starting Deep CyberDefender Crawler...")
    print("=" * 50)
    
    crawler = DeepCyberDefenderCrawler()
    
    try:
        crawler.crawl_deep(max_pages=500, delay=1)
        print("\nDeep crawling completed successfully!")
        print(f"Check the output directory for downloaded files:")
        print(f"  {crawler.output_dir}")
        print(f"Check the site map CSV:")
        print(f"  {crawler.output_dir / 'sitemap.csv'}")
        print(f"Check all discovered URLs:")
        print(f"  {crawler.output_dir / 'all_discovered_urls.txt'}")
        
    except KeyboardInterrupt:
        print("\nDeep crawling interrupted by user.")
    except Exception as e:
        print(f"\nError during deep crawling: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


