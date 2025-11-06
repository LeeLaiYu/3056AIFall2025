#!/usr/bin/env python3
"""
Complete Content Analyzer for CyberDefender.hk
Processes ALL 250 discovered URLs and creates comprehensive enhanced sitemap
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
from pathlib import Path
from datetime import datetime
import re
import csv

class CompleteContentAnalyzer:
    def __init__(self, output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\Anti-Scamming\\cytberdefender"):
        self.output_dir = Path(output_dir)
        self.urls_file = self.output_dir / "all_discovered_urls.txt"
        self.complete_enhanced_sitemap = self.output_dir / "complete_enhanced_sitemap.csv"
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / "complete_content_analyzer.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_page_summary(self, url):
        """Extract a one-line summary from a webpage"""
        try:
            self.logger.info(f"Analyzing: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to extract a meaningful summary
            summary = self.generate_summary(soup, url)
            
            return summary, response.status_code
            
        except Exception as e:
            self.logger.error(f"Failed to analyze {url}: {str(e)}")
            return f"Error: {str(e)[:100]}", "ERROR"
    
    def generate_summary(self, soup, url):
        """Generate a one-line summary from page content"""
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Try different methods to get meaningful content
        summary_parts = []
        
        # Method 1: Look for main heading
        main_heading = soup.find(['h1', 'h2'])
        if main_heading:
            heading_text = main_heading.get_text().strip()
            if len(heading_text) > 10 and len(heading_text) < 200:
                summary_parts.append(heading_text)
        
        # Method 2: Look for meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc.get('content').strip()
            if len(desc) > 10 and len(desc) < 200:
                summary_parts.append(desc)
        
        # Method 3: Look for first paragraph with substantial content
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 50 and len(text) < 200:
                # Clean up the text
                text = re.sub(r'\s+', ' ', text)
                summary_parts.append(text)
                break
        
        # Method 4: Look for specific content based on URL patterns
        if '/scameter' in url:
            summary_parts.append("Scameter tool for checking suspicious websites and phone numbers")
        elif '/phishing' in url:
            summary_parts.append("Information about phishing attacks and how to protect yourself")
        elif '/ransomware' in url:
            summary_parts.append("Guidance on ransomware threats and prevention measures")
        elif '/cryptocurrency' in url:
            summary_parts.append("Educational content about cryptocurrency and related risks")
        elif '/cyberbullying' in url:
            summary_parts.append("Resources about cyberbullying prevention and response")
        elif '/child' in url:
            summary_parts.append("Child protection resources and online safety guidelines")
        elif '/privacy' in url:
            summary_parts.append("Privacy protection tips and digital security advice")
        elif '/password' in url:
            summary_parts.append("Password security tools and best practices")
        elif '/firewall' in url:
            summary_parts.append("Firewall setup and network security guidance")
        elif '/wifi' in url:
            summary_parts.append("Public WiFi safety tips and security measures")
        elif '/romance' in url:
            summary_parts.append("Information about romance scams and how to avoid them")
        elif '/investment' in url:
            summary_parts.append("Investment fraud awareness and prevention tips")
        elif '/shopping' in url:
            summary_parts.append("Online shopping fraud prevention and safety guidelines")
        elif '/employment' in url:
            summary_parts.append("Online employment fraud awareness and prevention")
        elif '/account' in url:
            summary_parts.append("Account hijacking prevention and security measures")
        elif '/credit' in url:
            summary_parts.append("Credit card fraud prevention and security tips")
        elif '/artificial' in url:
            summary_parts.append("Artificial intelligence security and awareness")
        elif '/iot' in url:
            summary_parts.append("Internet of Things security and protection")
        elif '/cloud' in url:
            summary_parts.append("Cloud computing security and best practices")
        elif '/metaverse' in url:
            summary_parts.append("Metaverse security and digital safety")
        elif '/darkweb' in url:
            summary_parts.append("Dark web awareness and security information")
        elif '/deep' in url:
            summary_parts.append("Deepfake technology awareness and detection")
        elif '/cookie' in url:
            summary_parts.append("Cookie privacy and web tracking information")
        elif '/biometric' in url:
            summary_parts.append("Biometric security and authentication")
        elif '/recovery' in url:
            summary_parts.append("Recovery phrase security for cryptocurrency")
        elif '/social' in url:
            summary_parts.append("Social media security and privacy settings")
        elif '/public' in url:
            summary_parts.append("Public WiFi security and safety measures")
        elif '/hygiene' in url:
            summary_parts.append("Cyber hygiene and digital security best practices")
        elif '/dispose' in url:
            summary_parts.append("Safe disposal of old mobile phones and devices")
        elif '/iphone' in url:
            summary_parts.append("iPhone security and antivirus information")
        elif '/netiquette' in url:
            summary_parts.append("Online etiquette and digital citizenship")
        elif '/literacy' in url:
            summary_parts.append("Digital literacy and online safety education")
        elif '/abusive' in url:
            summary_parts.append("Abusive online content awareness and reporting")
        elif '/doxxing' in url:
            summary_parts.append("Doxxing prevention and online privacy protection")
        elif '/voyeurism' in url:
            summary_parts.append("Voyeurism awareness and prevention")
        elif '/grooming' in url:
            summary_parts.append("Online child grooming awareness and prevention")
        elif '/porn' in url:
            summary_parts.append("Child pornography awareness and reporting")
        elif '/cybersex' in url:
            summary_parts.append("Cybersex awareness and online safety")
        elif '/protection' in url:
            summary_parts.append("Child protection and online safety resources")
        elif '/guideline' in url:
            summary_parts.append("Teen internet guidelines and safety tips")
        elif '/learning' in url:
            summary_parts.append("Learning resources and educational materials")
        elif '/story' in url:
            summary_parts.append("Educational stories and case studies")
        elif '/ddos' in url:
            summary_parts.append("DDoS attack awareness and prevention")
        elif '/mitm' in url:
            summary_parts.append("Man-in-the-middle attack awareness and prevention")
        elif '/identity' in url:
            summary_parts.append("Identity theft prevention and protection")
        elif '/victim' in url:
            summary_parts.append("Victim mentality awareness and support")
        elif '/email' in url:
            summary_parts.append("Email scam awareness and prevention")
        elif '/naked' in url:
            summary_parts.append("Naked chat blackmail awareness and prevention")
        elif '/statistics' in url:
            summary_parts.append("Cybercrime statistics and data")
        elif '/activity' in url:
            summary_parts.append("Activities and events information")
        elif '/video' in url:
            summary_parts.append("Video resources and educational content")
        elif '/member' in url:
            summary_parts.append("Member login and account access")
        elif '/useful' in url:
            summary_parts.append("Useful links and resources")
        elif '/cspa' in url:
            summary_parts.append("CSPA 2025 campaign information")
        elif '/bughunting' in url:
            summary_parts.append("Bug hunting campaign and security research")
        elif '/page/' in url:
            summary_parts.append("Pagination page with multiple content items")
        elif '/disclaimer' in url:
            summary_parts.append("Website disclaimer and terms")
        elif '/privacy-policy' in url:
            summary_parts.append("Privacy policy and data protection")
        elif '/about' in url:
            summary_parts.append("About us and organization information")
        
        # Combine the best summary
        if summary_parts:
            # Take the first meaningful summary
            summary = summary_parts[0]
            # Clean up the summary
            summary = re.sub(r'\s+', ' ', summary)
            summary = summary.strip()
            
            # Truncate if too long
            if len(summary) > 200:
                summary = summary[:197] + "..."
            
            return summary
        else:
            # Fallback: use page title
            title = soup.find('title')
            if title:
                return title.get_text().strip()[:200]
            else:
                return "Content analysis completed - no specific summary available"
    
    def categorize_url(self, url):
        """Categorize URL based on path"""
        if '/it-basics' in url:
            return 'IT Basics'
        elif '/secure-your-device' in url:
            return 'Secure Your Device'
        elif '/parents-and-teachers' in url:
            return 'Parents & Teachers'
        elif '/cybercrime' in url:
            return 'Cybercrime'
        elif '/events' in url or '/resources' in url:
            return 'Resources & Events'
        elif '/scameter' in url:
            return 'Scameter'
        elif url.endswith('/') or 'cyberdefender.hk' in url and '/en-us' not in url:
            return 'Main Pages'
        else:
            return 'Other'
    
    def analyze_all_urls(self):
        """Analyze content for all 250 discovered URLs"""
        self.logger.info("Starting complete content analysis for all 250 URLs...")
        
        # Read all discovered URLs
        discovered_urls = []
        with open(self.urls_file, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url:
                    discovered_urls.append(url)
        
        self.logger.info(f"Found {len(discovered_urls)} URLs to analyze")
        
        # Create comprehensive enhanced sitemap
        with open(self.complete_enhanced_sitemap, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'URL', 'Filename', 'Title', 'Links Found', 'Status', 
                'Timestamp', 'Error (if failed)', 'Category', 'Page Summary'
            ])
            
            successful_count = 0
            failed_count = 0
            
            for i, url in enumerate(discovered_urls, 1):
                self.logger.info(f"Progress: {i}/{len(discovered_urls)} - {url}")
                
                # Extract summary
                summary, status_code = self.extract_page_summary(url)
                
                # Generate filename
                from urllib.parse import urlparse
                parsed = urlparse(url)
                path = parsed.path
                if not path or path == '/':
                    filename = 'index.html'
                else:
                    filename = path.lstrip('/').replace('/', '_')
                    if not filename.endswith(('.html', '.htm')):
                        filename += '.html'
                
                # Determine status
                if status_code == "ERROR":
                    status = "failed"
                    error_msg = summary
                    links_found = 0
                    title = "Failed Download"
                    successful_count += 0
                    failed_count += 1
                else:
                    status = "success"
                    error_msg = ""
                    links_found = "N/A"  # We don't have this info for new URLs
                    title = "Content Analyzed"
                    successful_count += 1
                    failed_count += 0
                
                # Categorize URL
                category = self.categorize_url(url)
                
                # Write row
                writer.writerow([
                    url,
                    filename,
                    title,
                    links_found,
                    status,
                    datetime.now().isoformat(),
                    error_msg,
                    category,
                    summary
                ])
                
                # Respectful delay
                time.sleep(1)
                
                if i % 25 == 0:
                    self.logger.info(f"Completed {i} URLs, {successful_count} successful, {failed_count} failed")
        
        self.logger.info(f"Complete content analysis finished!")
        self.logger.info(f"Total URLs processed: {len(discovered_urls)}")
        self.logger.info(f"Successful: {successful_count}")
        self.logger.info(f"Failed: {failed_count}")
        self.logger.info(f"Complete enhanced sitemap saved to: {self.complete_enhanced_sitemap}")

def main():
    """Main function to run complete content analysis"""
    print("Starting Complete Content Analysis for ALL 250 URLs...")
    print("=" * 60)
    
    analyzer = CompleteContentAnalyzer()
    
    try:
        analyzer.analyze_all_urls()
        
        print("\n" + "=" * 60)
        print("COMPLETE CONTENT ANALYSIS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Complete enhanced sitemap with ALL 250 URLs created:")
        print(f"  - complete_enhanced_sitemap.csv")
        print(f"  - complete_content_analyzer.log (detailed logs)")
        
    except KeyboardInterrupt:
        print("\nContent analysis interrupted by user.")
    except Exception as e:
        print(f"\nError during content analysis: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)


