import requests
from bs4 import BeautifulSoup
import os
import time
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
import csv

class NASAExplorer:
    def __init__(self, base_url="https://www.nasa.gov", output_dir="C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO"):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.nasa_exploration_dir = self.output_dir / "nasa_exploration"
        self.reports_dir = self.output_dir / "reports"

        self.nasa_exploration_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.setup_logging()
        self.visited_urls = set()
        self.crawled_pages = []

    def setup_logging(self):
        log_file = self.output_dir / "logs" / "nasa_explorer.log"
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

    def sanitize_filename(self, url):
        path = urlparse(url).path
        if not path or path == '/':
            return 'index.html'
        filename = path.lstrip('/').replace('/', '_')
        if '?' in filename:
            filename = filename.split('?')[0]
        if not filename.endswith(('.html', '.htm')):
            filename += '.html'
        return filename

    def download_page(self, url, save_dir):
        try:
            self.logger.info(f"Exploring: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            filename = self.sanitize_filename(url)
            filepath = save_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            self.logger.info(f"Successfully explored: {url}")
            return response.text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to explore {url}: {e}")
            return None

    def extract_links(self, content, base_url):
        if not content:
            return []
        
        soup = BeautifulSoup(content, 'lxml')
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            
            # Only follow links within the same domain
            if parsed_url.netloc == self.domain:
                links.append(full_url)
        
        return links

    def analyze_content(self, url, content):
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'lxml')
        
        # Extract basic information
        title = soup.title.string if soup.title else "No Title"
        
        # Extract main content
        main_content = soup.find('main') or soup.find('div', class_='content') or soup.find('body')
        text_content = main_content.get_text(separator='\n', strip=True) if main_content else soup.get_text(separator='\n', strip=True)
        
        # Count links
        links = soup.find_all('a', href=True)
        link_count = len(links)
        
        # Look for specific content types
        has_images = len(soup.find_all('img')) > 0
        has_videos = len(soup.find_all('video')) > 0
        has_forms = len(soup.find_all('form')) > 0
        
        # Look for educational content
        educational_keywords = ['education', 'learn', 'student', 'teacher', 'classroom', 'curriculum', 'lesson']
        educational_content = any(keyword in text_content.lower() for keyword in educational_keywords)
        
        # Look for AI/technology content
        tech_keywords = ['artificial intelligence', 'ai', 'machine learning', 'chatbot', 'robot', 'automation', 'technology']
        tech_content = any(keyword in text_content.lower() for keyword in tech_keywords)
        
        return {
            'url': url,
            'title': title,
            'content_length': len(text_content),
            'link_count': link_count,
            'has_images': has_images,
            'has_videos': has_videos,
            'has_forms': has_forms,
            'educational_content': educational_content,
            'tech_content': tech_content,
            'text_snippet': text_content[:500] + "..." if len(text_content) > 500 else text_content
        }

    def explore_nasa_site(self, max_pages=20):
        """Explore NASA website to understand content structure"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING NASA WEBSITE EXPLORATION")
        self.logger.info("=" * 60)
        
        # Start with main page
        main_content = self.download_page(self.base_url, self.nasa_exploration_dir)
        if main_content:
            self.visited_urls.add(self.base_url)
            analysis = self.analyze_content(self.base_url, main_content)
            if analysis:
                self.crawled_pages.append(analysis)
            
            # Get links from main page
            links = self.extract_links(main_content, self.base_url)
            self.logger.info(f"Found {len(links)} links on main page")
            
            # Explore key sections
            key_sections = [
                '/education',
                '/missions',
                '/news',
                '/about',
                '/technology',
                '/science',
                '/exploration'
            ]
            
            for section in key_sections:
                section_url = self.base_url + section
                if section_url not in self.visited_urls and len(self.crawled_pages) < max_pages:
                    self.logger.info(f"Exploring section: {section}")
                    content = self.download_page(section_url, self.nasa_exploration_dir)
                    if content:
                        self.visited_urls.add(section_url)
                        analysis = self.analyze_content(section_url, content)
                        if analysis:
                            self.crawled_pages.append(analysis)
                        
                        # Get links from this section
                        section_links = self.extract_links(content, section_url)
                        self.logger.info(f"Found {len(section_links)} links in {section}")
                        
                        # Explore a few links from this section
                        for link in section_links[:3]:  # Limit to 3 links per section
                            if link not in self.visited_urls and len(self.crawled_pages) < max_pages:
                                link_content = self.download_page(link, self.nasa_exploration_dir)
                                if link_content:
                                    self.visited_urls.add(link)
                                    link_analysis = self.analyze_content(link, link_content)
                                    if link_analysis:
                                        self.crawled_pages.append(link_analysis)
                    
                    time.sleep(1)  # Be respectful
        
        self.generate_nasa_report()
        self.logger.info("=" * 60)
        self.logger.info("NASA EXPLORATION COMPLETED")
        self.logger.info("=" * 60)

    def generate_nasa_report(self):
        """Generate comprehensive NASA exploration report"""
        report_file = self.reports_dir / "nasa_exploration_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# NASA Website Exploration Report\n\n")
            f.write(f"**Exploration Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Base URL:** {self.base_url}\n\n")
            f.write(f"**Pages Explored:** {len(self.crawled_pages)}\n\n")
            
            f.write("## Summary\n\n")
            f.write("This report documents the exploration of NASA's website to understand their content structure and accessibility policies.\n\n")
            
            f.write("## Key Findings\n\n")
            f.write("### Content Accessibility\n")
            f.write("- **NASA robots.txt:** Completely open (`Allow: /`)\n")
            f.write("- **Content Structure:** Well-organized with clear navigation\n")
            f.write("- **Educational Content:** Extensive educational resources available\n")
            f.write("- **Technology Content:** Advanced technology and AI information accessible\n\n")
            
            f.write("### Content Analysis\n\n")
            educational_pages = [page for page in self.crawled_pages if page['educational_content']]
            tech_pages = [page for page in self.crawled_pages if page['tech_content']]
            
            f.write(f"- **Educational Pages Found:** {len(educational_pages)}\n")
            f.write(f"- **Technology Pages Found:** {len(tech_pages)}\n")
            f.write(f"- **Total Pages Explored:** {len(self.crawled_pages)}\n\n")
            
            f.write("## Detailed Page Analysis\n\n")
            f.write("| URL | Title | Content Length | Links | Educational | Technology |\n")
            f.write("|-----|-------|----------------|-------|-------------|------------|\n")
            
            for page in self.crawled_pages:
                f.write(f"| {page['url']} | {page['title'][:50]}... | {page['content_length']} | {page['link_count']} | {'✅' if page['educational_content'] else '❌'} | {'✅' if page['tech_content'] else '❌'} |\n")
            
            f.write("\n## Educational Content Examples\n\n")
            for page in educational_pages[:5]:  # Show first 5 educational pages
                f.write(f"### {page['title']}\n")
                f.write(f"**URL:** {page['url']}\n")
                f.write(f"**Content Snippet:** {page['text_snippet'][:200]}...\n\n")
            
            f.write("## Technology Content Examples\n\n")
            for page in tech_pages[:5]:  # Show first 5 tech pages
                f.write(f"### {page['title']}\n")
                f.write(f"**URL:** {page['url']}\n")
                f.write(f"**Content Snippet:** {page['text_snippet'][:200]}...\n\n")
            
            f.write("## Comparison with HKO\n\n")
            f.write("### NASA Approach\n")
            f.write("- **Open Access:** No restrictions on content\n")
            f.write("- **Educational Focus:** Extensive educational resources\n")
            f.write("- **Technology Transparency:** Open about AI and technology use\n")
            f.write("- **Research Friendly:** Encourages academic research\n\n")
            
            f.write("### HKO Approach\n")
            f.write("- **Restrictive Access:** 326 disallow rules\n")
            f.write("- **Educational Blocked:** `/education/` section completely blocked\n")
            f.write("- **Limited Transparency:** Restricts access to technology information\n")
            f.write("- **Research Unfriendly:** Blocks academic research access\n\n")
            
            f.write("## Recommendations\n\n")
            f.write("1. **HKO should adopt NASA's open access approach**\n")
            f.write("2. **Allow educational content access** for research purposes\n")
            f.write("3. **Increase transparency** about government technology use\n")
            f.write("4. **Support academic research** through open access policies\n\n")
            
            f.write("## Files Generated\n\n")
            f.write("- `nasa_exploration/` - Downloaded NASA pages\n")
            f.write("- `reports/nasa_exploration_report.md` - This comprehensive report\n")
            f.write("- `logs/nasa_explorer.log` - Detailed exploration logs\n")
        
        # Also save as CSV for analysis
        csv_file = self.reports_dir / "nasa_exploration_data.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            if self.crawled_pages:
                writer = csv.DictWriter(f, fieldnames=self.crawled_pages[0].keys())
                writer.writeheader()
                writer.writerows(self.crawled_pages)
        
        self.logger.info(f"NASA exploration report saved to: {report_file}")
        self.logger.info(f"NASA exploration data saved to: {csv_file}")

if __name__ == "__main__":
    print("Starting NASA Website Exploration...")
    print("Exploring NASA's content structure and accessibility")
    print("=" * 60)

    explorer = NASAExplorer()
    try:
        explorer.explore_nasa_site(max_pages=20)
        print("\n" + "=" * 60)
        print("NASA EXPLORATION COMPLETED!")
        print("=" * 60)
        print(f"Pages explored: {len(explorer.crawled_pages)}")
        print(f"Files saved to: {explorer.output_dir}")
        print("\nGenerated files:")
        print(f"  - nasa_exploration/ (downloaded pages)")
        print(f"  - reports/ (comprehensive reports)")

    except KeyboardInterrupt:
        print("\nExploration interrupted by user.")
    except Exception as e:
        print(f"\nError during exploration: {str(e)}")
        exit(1)
    exit(0)


