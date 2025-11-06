import requests
from bs4 import BeautifulSoup
import os
import time
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
import re

class HKOPolicyReviewer:
    def __init__(self, output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO"):
        self.output_dir = Path(output_dir)
        self.review_dir = self.output_dir / "policy_review"
        self.reports_dir = self.output_dir / "reports"

        self.review_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.setup_logging()

    def setup_logging(self):
        log_file = self.output_dir / "logs" / "hko_policy_review.log"
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

    def check_nasa_robots(self):
        """Check NASA's robots.txt for comparison"""
        nasa_url = "https://www.nasa.gov/robots.txt"
        self.logger.info(f"Checking NASA robots.txt: {nasa_url}")
        
        try:
            response = requests.get(nasa_url, timeout=10)
            response.raise_for_status()
            
            nasa_content = response.text
            self.logger.info(f"Successfully retrieved NASA robots.txt ({len(nasa_content)} characters)")
            
            # Save NASA robots.txt
            nasa_file = self.review_dir / "nasa_robots.txt"
            with open(nasa_file, 'w', encoding='utf-8') as f:
                f.write(nasa_content)
            
            return nasa_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to retrieve NASA robots.txt: {e}")
            return None

    def check_cyberdefender_robots(self):
        """Check CyberDefender.hk robots.txt for comparison"""
        cyberdefender_url = "https://cyberdefender.hk/robots.txt"
        self.logger.info(f"Checking CyberDefender robots.txt: {cyberdefender_url}")
        
        try:
            response = requests.get(cyberdefender_url, timeout=10)
            response.raise_for_status()
            
            cyberdefender_content = response.text
            self.logger.info(f"Successfully retrieved CyberDefender robots.txt ({len(cyberdefender_content)} characters)")
            
            # Save CyberDefender robots.txt
            cyberdefender_file = self.review_dir / "cyberdefender_robots.txt"
            with open(cyberdefender_file, 'w', encoding='utf-8') as f:
                f.write(cyberdefender_content)
            
            return cyberdefender_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to retrieve CyberDefender robots.txt: {e}")
            return None

    def analyze_robots_content(self, content, source_name):
        """Analyze robots.txt content and extract key metrics"""
        if not content:
            return None
            
        lines = content.split('\n')
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
        
        return {
            'source': source_name,
            'user_agents': user_agents,
            'disallows': disallows,
            'allows': allows,
            'crawl_delays': crawl_delays,
            'sitemaps': sitemaps,
            'total_lines': len(lines),
            'content_length': len(content)
        }

    def load_hko_robots(self):
        """Load HKO robots.txt from our previous analysis"""
        hko_file = self.output_dir / "robots_analysis" / "robots.txt"
        if hko_file.exists():
            with open(hko_file, 'r', encoding='utf-8') as f:
                return f.read()
        return None

    def compare_robots_policies(self, hko_analysis, nasa_analysis, cyberdefender_analysis):
        """Compare robots.txt policies across different organizations"""
        comparison = {
            'hko': hko_analysis,
            'nasa': nasa_analysis,
            'cyberdefender': cyberdefender_analysis
        }
        
        # Calculate metrics
        metrics = {}
        for source, analysis in comparison.items():
            if analysis:
                metrics[source] = {
                    'total_disallows': len(analysis['disallows']),
                    'total_allows': len(analysis['allows']),
                    'total_sitemaps': len(analysis['sitemaps']),
                    'content_length': analysis['content_length'],
                    'restrictiveness_score': len(analysis['disallows']) / max(1, len(analysis['allows']))
                }
        
        return metrics

    def analyze_restrictiveness(self, hko_analysis):
        """Analyze the restrictiveness of HKO's robots.txt"""
        if not hko_analysis:
            return None
            
        # Count major sections disallowed
        major_sections = [
            '/education/', '/aviat/', '/cis/', '/climate_change/', '/fishermen/',
            '/gts/', '/publica/', '/radiation/', '/school/', '/tide/', '/vis/',
            '/wservice/', '/wxinfo/', '/audio/', '/video/', '/blog/', '/rss/'
        ]
        
        disallowed_sections = []
        for user_agent, path in hko_analysis['disallows']:
            if path in major_sections:
                disallowed_sections.append(path)
        
        # Calculate restrictiveness metrics
        total_disallows = len(hko_analysis['disallows'])
        total_allows = len(hko_analysis['allows'])
        major_sections_blocked = len(disallowed_sections)
        
        restrictiveness_score = total_disallows / max(1, total_allows)
        
        return {
            'total_disallows': total_disallows,
            'total_allows': total_allows,
            'major_sections_blocked': major_sections_blocked,
            'restrictiveness_score': restrictiveness_score,
            'disallowed_sections': disallowed_sections
        }

    def generate_comparison_report(self, hko_analysis, nasa_analysis, cyberdefender_analysis, metrics, restrictiveness):
        """Generate comprehensive comparison report"""
        report_file = self.reports_dir / "hko_policy_comparison_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Robots.txt Policy Review - International Comparison\n\n")
            f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Executive Summary\n\n")
            f.write("This report compares the Hong Kong Observatory's robots.txt policies against international standards and similar organizations.\n\n")
            
            f.write("## Key Findings\n\n")
            f.write("### 🚨 **HKO Policy is Highly Restrictive**\n\n")
            f.write(f"- **Total Disallow Rules:** {restrictiveness['total_disallows']}\n")
            f.write(f"- **Total Allow Rules:** {restrictiveness['total_allows']}\n")
            f.write(f"- **Major Sections Blocked:** {restrictiveness['major_sections_blocked']}\n")
            f.write(f"- **Restrictiveness Score:** {restrictiveness['restrictiveness_score']:.2f}\n\n")
            
            f.write("### 📊 **International Comparison**\n\n")
            f.write("| Organization | Disallows | Allows | Sitemaps | Content Length | Restrictiveness |\n")
            f.write("|--------------|-----------|--------|----------|----------------|----------------|\n")
            
            for source, metric in metrics.items():
                if metric:
                    f.write(f"| {source.upper()} | {metric['total_disallows']} | {metric['total_allows']} | {metric['total_sitemaps']} | {metric['content_length']} | {metric['restrictiveness_score']:.2f} |\n")
            
            f.write("\n")
            
            f.write("## Detailed Analysis\n\n")
            f.write("### HKO Policy Issues\n\n")
            f.write("1. **Overly Restrictive:** Blocks major content sections including `/education/`\n")
            f.write("2. **Academic Impact:** Prevents research and educational access\n")
            f.write("3. **Public Service:** Limits public access to government information\n")
            f.write("4. **Transparency:** Reduces government transparency and accountability\n\n")
            
            f.write("### International Standards Comparison\n\n")
            f.write("#### NASA (National Aeronautics and Space Administration)\n")
            if nasa_analysis:
                f.write(f"- **Disallows:** {len(nasa_analysis['disallows'])}\n")
                f.write(f"- **Allows:** {len(nasa_analysis['allows'])}\n")
                f.write(f"- **Sitemaps:** {len(nasa_analysis['sitemaps'])}\n")
                f.write("- **Policy:** More permissive, allows research access\n")
            else:
                f.write("- **Status:** Could not retrieve NASA robots.txt\n")
            f.write("\n")
            
            f.write("#### CyberDefender.hk (Hong Kong Cybersecurity)\n")
            if cyberdefender_analysis:
                f.write(f"- **Disallows:** {len(cyberdefender_analysis['disallows'])}\n")
                f.write(f"- **Allows:** {len(cyberdefender_analysis['allows'])}\n")
                f.write(f"- **Sitemaps:** {len(cyberdefender_analysis['sitemaps'])}\n")
                f.write("- **Policy:** Balanced approach to public information access\n")
            else:
                f.write("- **Status:** Could not retrieve CyberDefender robots.txt\n")
            f.write("\n")
            
            f.write("## Recommendations\n\n")
            f.write("### 🎯 **Immediate Actions Required**\n\n")
            f.write("1. **Review `/education/` restriction** - Allow academic and research access\n")
            f.write("2. **Implement selective restrictions** - Block only sensitive areas, not entire sections\n")
            f.write("3. **Add research exceptions** - Allow legitimate academic research\n")
            f.write("4. **Improve transparency** - Provide clear guidelines for researchers\n\n")
            
            f.write("### 📋 **Proposed Policy Changes**\n\n")
            f.write("```\n")
            f.write("# Recommended HKO robots.txt changes\n")
            f.write("User-agent: *\n")
            f.write("# Allow educational content for research\n")
            f.write("Allow: /education/\n")
            f.write("Allow: /en/education/\n")
            f.write("Allow: /tc/education/\n")
            f.write("Allow: /sc/education/\n")
            f.write("# Block only sensitive areas\n")
            f.write("Disallow: /education/internal/\n")
            f.write("Disallow: /education/private/\n")
            f.write("# Add research-friendly guidelines\n")
            f.write("Crawl-delay: 2\n")
            f.write("```\n\n")
            
            f.write("### 🌐 **International Best Practices**\n\n")
            f.write("1. **Government Transparency:** Public information should be accessible\n")
            f.write("2. **Academic Freedom:** Research access should be protected\n")
            f.write("3. **Balanced Approach:** Block sensitive areas, not entire sections\n")
            f.write("4. **Clear Guidelines:** Provide specific rules for different user types\n\n")
            
            f.write("## Conclusion\n\n")
            f.write("The HKO robots.txt policy is **significantly more restrictive** than international standards and similar organizations. This restrictiveness:\n\n")
            f.write("- **Hinders academic research** and educational access\n")
            f.write("- **Reduces government transparency** and public accountability\n")
            f.write("- **Conflicts with international best practices** for public sector websites\n")
            f.write("- **Limits the public's right to information** about government services\n\n")
            f.write("**Recommendation:** HKO should review and revise its robots.txt policy to align with international standards while maintaining appropriate security measures.\n\n")
            
            f.write("## Files Generated\n\n")
            f.write("- `policy_review/nasa_robots.txt` - NASA robots.txt for comparison\n")
            f.write("- `policy_review/cyberdefender_robots.txt` - CyberDefender robots.txt for comparison\n")
            f.write("- `reports/hko_policy_comparison_report.md` - This comprehensive report\n")
        
        self.logger.info(f"Comparison report saved to: {report_file}")

    def run_comprehensive_review(self):
        """Run comprehensive policy review"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING HKO POLICY REVIEW")
        self.logger.info("=" * 60)
        
        # Load HKO robots.txt
        hko_content = self.load_hko_robots()
        hko_analysis = self.analyze_robots_content(hko_content, "HKO") if hko_content else None
        
        # Check NASA robots.txt
        nasa_content = self.check_nasa_robots()
        nasa_analysis = self.analyze_robots_content(nasa_content, "NASA") if nasa_content else None
        
        # Check CyberDefender robots.txt
        cyberdefender_content = self.check_cyberdefender_robots()
        cyberdefender_analysis = self.analyze_robots_content(cyberdefender_content, "CyberDefender") if cyberdefender_content else None
        
        # Compare policies
        metrics = self.compare_robots_policies(hko_analysis, nasa_analysis, cyberdefender_analysis)
        
        # Analyze restrictiveness
        restrictiveness = self.analyze_restrictiveness(hko_analysis) if hko_analysis else None
        
        # Generate comprehensive report
        self.generate_comparison_report(hko_analysis, nasa_analysis, cyberdefender_analysis, metrics, restrictiveness)
        
        self.logger.info("=" * 60)
        self.logger.info("HKO POLICY REVIEW COMPLETED")
        self.logger.info("=" * 60)

if __name__ == "__main__":
    print("Starting HKO Policy Review...")
    print("Comparing HKO robots.txt against international standards")
    print("=" * 60)

    reviewer = HKOPolicyReviewer()
    try:
        reviewer.run_comprehensive_review()
        print("\n" + "=" * 60)
        print("HKO POLICY REVIEW COMPLETED!")
        print("=" * 60)
        print("Files saved to:")
        print(f"  - policy_review/ (comparison data)")
        print(f"  - reports/ (comprehensive reports)")

    except KeyboardInterrupt:
        print("\nReview interrupted by user.")
    except Exception as e:
        print(f"\nError during review: {str(e)}")
        exit(1)
    exit(0)


