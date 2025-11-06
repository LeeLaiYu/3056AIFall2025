import requests
from bs4 import BeautifulSoup
import os
import time
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime

class HKORobotsChecker:
    def __init__(self, base_url="https://www.hko.gov.hk", output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO"):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.robots_dir = self.output_dir / "robots_analysis"
        self.reports_dir = self.output_dir / "reports"

        self.robots_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.setup_logging()

    def setup_logging(self):
        log_file = self.output_dir / "logs" / "hko_robots_checker.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def check_robots_txt(self):
        """Check robots.txt file for crawling policies"""
        robots_url = f"{self.base_url}/robots.txt"
        self.logger.info(f"Checking robots.txt: {robots_url}")
        
        try:
            response = requests.get(robots_url, timeout=10)
            response.raise_for_status()
            
            robots_content = response.text
            self.logger.info(f"Successfully retrieved robots.txt ({len(robots_content)} characters)")
            
            # Save robots.txt content
            robots_file = self.robots_dir / "robots.txt"
            with open(robots_file, 'w', encoding='utf-8') as f:
                f.write(robots_content)
            
            # Analyze robots.txt
            self.analyze_robots_txt(robots_content)
            
            return robots_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to retrieve robots.txt: {e}")
            return None

    def analyze_robots_txt(self, robots_content):
        """Analyze robots.txt content for crawling policies"""
        self.logger.info("Analyzing robots.txt content...")
        
        lines = robots_content.split('\n')
        user_agents = []
        disallows = []
        allows = []
        crawl_delays = []
        sitemaps = []
        
        current_user_agent = None
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            if line.lower().startswith('user-agent:'):
                current_user_agent = line.split(':', 1)[1].strip()
                user_agents.append(current_user_agent)
                
            elif line.lower().startswith('disallow:'):
                disallow_path = line.split(':', 1)[1].strip()
                if disallow_path:
                    disallows.append((current_user_agent, disallow_path))
                    
            elif line.lower().startswith('allow:'):
                allow_path = line.split(':', 1)[1].strip()
                if allow_path:
                    allows.append((current_user_agent, allow_path))
                    
            elif line.lower().startswith('crawl-delay:'):
                delay = line.split(':', 1)[1].strip()
                crawl_delays.append((current_user_agent, delay))
                
            elif line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                sitemaps.append(sitemap_url)
        
        # Generate analysis report
        self.generate_robots_analysis_report(user_agents, disallows, allows, crawl_delays, sitemaps)
        
        return {
            'user_agents': user_agents,
            'disallows': disallows,
            'allows': allows,
            'crawl_delays': crawl_delays,
            'sitemaps': sitemaps
        }

    def check_sitemap(self, sitemap_url):
        """Check sitemap.xml for additional crawling information"""
        self.logger.info(f"Checking sitemap: {sitemap_url}")
        
        try:
            response = requests.get(sitemap_url, timeout=10)
            response.raise_for_status()
            
            sitemap_content = response.text
            self.logger.info(f"Successfully retrieved sitemap ({len(sitemap_content)} characters)")
            
            # Save sitemap content
            sitemap_file = self.robots_dir / "sitemap.xml"
            with open(sitemap_file, 'w', encoding='utf-8') as f:
                f.write(sitemap_content)
            
            # Analyze sitemap
            self.analyze_sitemap(sitemap_content, sitemap_url)
            
            return sitemap_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to retrieve sitemap: {e}")
            return None

    def analyze_sitemap(self, sitemap_content, sitemap_url):
        """Analyze sitemap.xml content"""
        self.logger.info("Analyzing sitemap content...")
        
        try:
            soup = BeautifulSoup(sitemap_content, 'xml')
            
            # Count URLs
            urls = soup.find_all('url')
            url_count = len(urls)
            
            # Get lastmod dates
            lastmod_dates = []
            for url in urls:
                lastmod = url.find('lastmod')
                if lastmod:
                    lastmod_dates.append(lastmod.text)
            
            # Get changefreq
            changefreqs = []
            for url in urls:
                changefreq = url.find('changefreq')
                if changefreq:
                    changefreqs.append(changefreq.text)
            
            # Get priorities
            priorities = []
            for url in urls:
                priority = url.find('priority')
                if priority:
                    priorities.append(float(priority.text))
            
            # Generate sitemap analysis report
            self.generate_sitemap_analysis_report(sitemap_url, url_count, lastmod_dates, changefreqs, priorities)
            
        except Exception as e:
            self.logger.error(f"Error analyzing sitemap: {e}")

    def check_other_policy_files(self):
        """Check for other policy files like terms of service, privacy policy, etc."""
        policy_urls = [
            f"{self.base_url}/terms",
            f"{self.base_url}/privacy",
            f"{self.base_url}/copyright",
            f"{self.base_url}/disclaimer",
            f"{self.base_url}/legal",
            f"{self.base_url}/about",
            f"{self.base_url}/contact"
        ]
        
        policy_results = {}
        
        for url in policy_urls:
            self.logger.info(f"Checking policy file: {url}")
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    policy_results[url] = {
                        'status': 'found',
                        'content_length': len(response.text),
                        'content_type': response.headers.get('content-type', 'unknown')
                    }
                    self.logger.info(f"Found policy file: {url}")
                else:
                    policy_results[url] = {'status': 'not_found', 'code': response.status_code}
            except requests.exceptions.RequestException as e:
                policy_results[url] = {'status': 'error', 'error': str(e)}
                self.logger.error(f"Error checking {url}: {e}")
        
        return policy_results

    def generate_robots_analysis_report(self, user_agents, disallows, allows, crawl_delays, sitemaps):
        """Generate comprehensive robots.txt analysis report"""
        report_file = self.robots_dir / "robots_analysis_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Robots.txt Analysis Report\n\n")
            f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Target Domain:** {self.domain}\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- **User Agents:** {len(user_agents)}\n")
            f.write(f"- **Disallow Rules:** {len(disallows)}\n")
            f.write(f"- **Allow Rules:** {len(allows)}\n")
            f.write(f"- **Crawl Delays:** {len(crawl_delays)}\n")
            f.write(f"- **Sitemaps:** {len(sitemaps)}\n\n")
            
            f.write("## User Agents\n\n")
            for agent in user_agents:
                f.write(f"- `{agent}`\n")
            f.write("\n")
            
            f.write("## Disallow Rules\n\n")
            for user_agent, path in disallows:
                f.write(f"- **{user_agent}:** `{path}`\n")
            f.write("\n")
            
            f.write("## Allow Rules\n\n")
            for user_agent, path in allows:
                f.write(f"- **{user_agent}:** `{path}`\n")
            f.write("\n")
            
            f.write("## Crawl Delays\n\n")
            for user_agent, delay in crawl_delays:
                f.write(f"- **{user_agent}:** {delay} seconds\n")
            f.write("\n")
            
            f.write("## Sitemaps\n\n")
            for sitemap in sitemaps:
                f.write(f"- {sitemap}\n")
            f.write("\n")
            
            f.write("## Crawling Recommendations\n\n")
            f.write("Based on the robots.txt analysis:\n\n")
            f.write("1. **Respect crawl delays** if specified\n")
            f.write("2. **Avoid disallowed paths** for the user agent being used\n")
            f.write("3. **Use sitemaps** for efficient crawling\n")
            f.write("4. **Be respectful** of server resources\n")
            f.write("5. **Monitor for changes** in robots.txt\n\n")
        
        self.logger.info(f"Robots analysis report saved to: {report_file}")

    def generate_sitemap_analysis_report(self, sitemap_url, url_count, lastmod_dates, changefreqs, priorities):
        """Generate sitemap analysis report"""
        report_file = self.robots_dir / "sitemap_analysis_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Sitemap Analysis Report\n\n")
            f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Sitemap URL:** {sitemap_url}\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- **Total URLs:** {url_count}\n")
            f.write(f"- **Last Modified Dates:** {len(lastmod_dates)}\n")
            f.write(f"- **Change Frequencies:** {len(changefreqs)}\n")
            f.write(f"- **Priorities:** {len(priorities)}\n\n")
            
            if lastmod_dates:
                f.write("## Last Modified Analysis\n\n")
                f.write(f"- **Oldest:** {min(lastmod_dates)}\n")
                f.write(f"- **Newest:** {max(lastmod_dates)}\n\n")
            
            if changefreqs:
                f.write("## Change Frequency Analysis\n\n")
                freq_counts = {}
                for freq in changefreqs:
                    freq_counts[freq] = freq_counts.get(freq, 0) + 1
                
                for freq, count in sorted(freq_counts.items()):
                    f.write(f"- **{freq}:** {count} URLs\n")
                f.write("\n")
            
            if priorities:
                f.write("## Priority Analysis\n\n")
                f.write(f"- **Average Priority:** {sum(priorities)/len(priorities):.2f}\n")
                f.write(f"- **Highest Priority:** {max(priorities)}\n")
                f.write(f"- **Lowest Priority:** {min(priorities)}\n\n")
        
        self.logger.info(f"Sitemap analysis report saved to: {report_file}")

    def run_comprehensive_analysis(self):
        """Run comprehensive robots.txt and policy analysis"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING HKO ROBOTS.TXT ANALYSIS")
        self.logger.info("=" * 60)
        
        # Check robots.txt
        robots_content = self.check_robots_txt()
        
        # Check sitemaps if found
        if robots_content:
            analysis = self.analyze_robots_txt(robots_content)
            sitemaps = analysis.get('sitemaps', [])
            
            for sitemap_url in sitemaps:
                self.check_sitemap(sitemap_url)
                time.sleep(1)  # Be polite
        
        # Check other policy files
        policy_results = self.check_other_policy_files()
        
        # Generate final report
        self.generate_final_report(robots_content, policy_results)
        
        self.logger.info("=" * 60)
        self.logger.info("HKO ROBOTS.TXT ANALYSIS COMPLETED")
        self.logger.info("=" * 60)

    def generate_final_report(self, robots_content, policy_results):
        """Generate final comprehensive report"""
        report_file = self.reports_dir / "hko_crawling_policies_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Crawling Policies Report\n\n")
            f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Target Domain:** {self.domain}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write("This report analyzes the Hong Kong Observatory's crawling policies and guidelines.\n\n")
            
            f.write("## Robots.txt Analysis\n\n")
            if robots_content:
                f.write("✅ **Robots.txt found and analyzed**\n\n")
                f.write("### Key Findings:\n")
                f.write("- Robots.txt file is accessible\n")
                f.write("- Contains crawling guidelines for different user agents\n")
                f.write("- Includes sitemap references\n")
                f.write("- Specifies disallow and allow rules\n\n")
            else:
                f.write("❌ **Robots.txt not accessible**\n\n")
            
            f.write("## Policy Files Analysis\n\n")
            found_policies = [url for url, result in policy_results.items() if result.get('status') == 'found']
            if found_policies:
                f.write("✅ **Policy files found:**\n")
                for policy_url in found_policies:
                    f.write(f"- {policy_url}\n")
                f.write("\n")
            else:
                f.write("❌ **No policy files found**\n\n")
            
            f.write("## Crawling Recommendations\n\n")
            f.write("1. **Always check robots.txt** before crawling\n")
            f.write("2. **Respect crawl delays** if specified\n")
            f.write("3. **Use appropriate user agent** identification\n")
            f.write("4. **Be respectful** of server resources\n")
            f.write("5. **Monitor for policy changes**\n\n")
            
            f.write("## Files Generated\n\n")
            f.write("- `robots_analysis/robots.txt` - Original robots.txt file\n")
            f.write("- `robots_analysis/robots_analysis_report.md` - Detailed robots.txt analysis\n")
            f.write("- `robots_analysis/sitemap.xml` - Sitemap file (if found)\n")
            f.write("- `robots_analysis/sitemap_analysis_report.md` - Sitemap analysis\n")
            f.write("- `reports/hko_crawling_policies_report.md` - This comprehensive report\n")
        
        self.logger.info(f"Final report saved to: {report_file}")

if __name__ == "__main__":
    print("Starting HKO Robots.txt and Policy Analysis...")
    print("Checking crawling policies and guidelines")
    print("=" * 60)

    checker = HKORobotsChecker()
    try:
        checker.run_comprehensive_analysis()
        print("\n" + "=" * 60)
        print("HKO ROBOTS.TXT ANALYSIS COMPLETED!")
        print("=" * 60)
        print("Files saved to:")
        print(f"  - robots_analysis/ (robots.txt and analysis)")
        print(f"  - reports/ (comprehensive reports)")

    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        exit(1)
    exit(0)


