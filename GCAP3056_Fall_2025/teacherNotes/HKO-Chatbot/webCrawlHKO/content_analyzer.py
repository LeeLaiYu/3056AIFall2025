#!/usr/bin/env python3
"""
Content Analyzer for HKO Website
Specialized analyzer to find Dr Tin chatbot mentions and related content
"""

import re
import json
from datetime import datetime
from pathlib import Path
import logging

class HKOContentAnalyzer:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.dr_tin_patterns = [
            # Direct mentions
            r'dr\s+tin\s+chatbot',
            r'dr\s+tin\s+bot',
            r'dr\.\s*tin\s+chatbot',
            r'dr\.\s*tin\s+bot',
            r'dr\s+tin\s+ai',
            r'dr\s+tin\s+assistant',
            
            # Chatbot related
            r'chatbot.*dr\s+tin',
            r'bot.*dr\s+tin',
            r'ai.*dr\s+tin',
            r'assistant.*dr\s+tin',
            
            # Reverse patterns
            r'dr\s+tin.*chatbot',
            r'dr\s+tin.*bot',
            r'dr\s+tin.*ai',
            r'dr\s+tin.*assistant',
            
            # Weather related chatbot
            r'weather\s+chatbot',
            r'weather\s+bot',
            r'weather\s+ai',
            r'weather\s+assistant',
            
            # HKO specific patterns
            r'hko\s+chatbot',
            r'hko\s+bot',
            r'observatory\s+chatbot',
            r'observatory\s+bot',
            
            # Dr Tin variations
            r'dr\s+tin\s+weather',
            r'dr\s+tin\s+forecast',
            r'dr\s+tin\s+meteorology'
        ]
        
        self.related_keywords = [
            'chatbot', 'bot', 'ai', 'artificial intelligence', 'assistant',
            'weather', 'forecast', 'meteorology', 'observatory', 'hko',
            'dr tin', 'tin', 'weatherman', 'meteorologist'
        ]
        
        self.analysis_results = []
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging for content analyzer"""
        log_file = self.output_dir / "content_analyzer.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_content(self, content, url, filename):
        """Analyze content for Dr Tin chatbot mentions and related content"""
        content_lower = content.lower()
        analysis = {
            'url': url,
            'filename': filename,
            'timestamp': datetime.now().isoformat(),
            'dr_tin_mentions': [],
            'related_keywords': [],
            'relevance_score': 0,
            'has_dr_tin_mention': False
        }
        
        # Check for Dr Tin patterns
        for pattern in self.dr_tin_patterns:
            matches = re.finditer(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                context_start = max(0, match.start() - 150)
                context_end = min(len(content), match.end() + 150)
                context = content[context_start:context_end].strip()
                
                analysis['dr_tin_mentions'].append({
                    'pattern': pattern,
                    'match': match.group(),
                    'context': context,
                    'position': match.start(),
                    'confidence': self.calculate_confidence(match.group(), context)
                })
                analysis['has_dr_tin_mention'] = True
        
        # Check for related keywords
        for keyword in self.related_keywords:
            if keyword in content_lower:
                count = content_lower.count(keyword)
                analysis['related_keywords'].append({
                    'keyword': keyword,
                    'count': count,
                    'positions': [m.start() for m in re.finditer(keyword, content_lower)]
                })
        
        # Calculate relevance score
        analysis['relevance_score'] = self.calculate_relevance_score(analysis)
        
        if analysis['has_dr_tin_mention'] or analysis['relevance_score'] > 0.3:
            self.analysis_results.append(analysis)
            self.logger.info(f"Relevant content found in: {url}")
        
        return analysis
    
    def calculate_confidence(self, match, context):
        """Calculate confidence score for a match"""
        confidence = 0.5  # Base confidence
        
        # Higher confidence for exact "dr tin" mentions
        if 'dr tin' in match.lower():
            confidence += 0.3
        
        # Higher confidence for chatbot-related context
        chatbot_words = ['chatbot', 'bot', 'ai', 'assistant', 'weather', 'forecast']
        for word in chatbot_words:
            if word in context.lower():
                confidence += 0.1
        
        return min(confidence, 1.0)
    
    def calculate_relevance_score(self, analysis):
        """Calculate overall relevance score for the content"""
        score = 0.0
        
        # Dr Tin mentions get highest score
        if analysis['has_dr_tin_mention']:
            score += 0.8
        
        # Related keywords contribute to score
        keyword_count = len(analysis['related_keywords'])
        if keyword_count > 0:
            score += min(keyword_count * 0.1, 0.4)
        
        # High keyword density increases score
        total_keywords = sum(kw['count'] for kw in analysis['related_keywords'])
        if total_keywords > 5:
            score += 0.2
        
        return min(score, 1.0)
    
    def generate_analysis_report(self):
        """Generate comprehensive analysis report"""
        report_file = self.output_dir / "reports" / "content_analysis_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# HKO Content Analysis Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Total pages analyzed:** {len(self.analysis_results)}\n\n")
            
            # Summary statistics
            dr_tin_pages = [a for a in self.analysis_results if a['has_dr_tin_mention']]
            high_relevance = [a for a in self.analysis_results if a['relevance_score'] > 0.5]
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Pages with Dr Tin mentions:** {len(dr_tin_pages)}\n")
            f.write(f"- **High relevance pages:** {len(high_relevance)}\n")
            f.write(f"- **Total relevant pages:** {len(self.analysis_results)}\n\n")
            
            # Dr Tin mentions
            if dr_tin_pages:
                f.write("## Dr Tin Chatbot Mentions\n\n")
                for i, analysis in enumerate(dr_tin_pages, 1):
                    f.write(f"### {i}. [{analysis['url']}]({analysis['url']})\n\n")
                    f.write(f"**Filename:** {analysis['filename']}\n")
                    f.write(f"**Relevance Score:** {analysis['relevance_score']:.2f}\n\n")
                    
                    f.write("**Mentions Found:**\n")
                    for j, mention in enumerate(analysis['dr_tin_mentions'], 1):
                        f.write(f"{j}. **Pattern:** `{mention['pattern']}`\n")
                        f.write(f"   **Match:** `{mention['match']}`\n")
                        f.write(f"   **Confidence:** {mention['confidence']:.2f}\n")
                        f.write(f"   **Context:** {mention['context']}\n\n")
                    
                    f.write("---\n\n")
            else:
                f.write("## No Dr Tin Chatbot Mentions Found\n\n")
                f.write("The analysis did not find any direct mentions of 'Dr Tin chatbot' on the HKO website.\n\n")
            
            # High relevance pages
            if high_relevance:
                f.write("## High Relevance Pages\n\n")
                for i, analysis in enumerate(high_relevance, 1):
                    f.write(f"### {i}. [{analysis['url']}]({analysis['url']})\n\n")
                    f.write(f"**Relevance Score:** {analysis['relevance_score']:.2f}\n")
                    f.write(f"**Related Keywords:** {', '.join([kw['keyword'] for kw in analysis['related_keywords']])}\n\n")
            
            # Keyword analysis
            f.write("## Keyword Analysis\n\n")
            keyword_stats = {}
            for analysis in self.analysis_results:
                for kw in analysis['related_keywords']:
                    keyword = kw['keyword']
                    if keyword not in keyword_stats:
                        keyword_stats[keyword] = 0
                    keyword_stats[keyword] += kw['count']
            
            f.write("**Most frequently mentioned keywords:**\n")
            for keyword, count in sorted(keyword_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
                f.write(f"- **{keyword}:** {count} mentions\n")
        
        self.logger.info(f"Analysis report saved to: {report_file}")
    
    def save_analysis_data(self):
        """Save analysis data as JSON for further processing"""
        json_file = self.output_dir / "reports" / "content_analysis_data.json"
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Analysis data saved to: {json_file}")

def main():
    """Test the content analyzer"""
    analyzer = HKOContentAnalyzer("C:\\Users\\simonwang\\Documents\\Vault4sync\\01-Courses\\GCAP3056\\HKO-Chatbot\\webCrawlHKO")
    
    # Test with sample content
    sample_content = """
    The Hong Kong Observatory has introduced a new Dr Tin chatbot to help users
    get weather information. This AI assistant can provide weather forecasts
    and answer questions about meteorology. The chatbot is available 24/7.
    """
    
    result = analyzer.analyze_content(sample_content, "https://example.com", "test.html")
    print(f"Analysis result: {result}")
    
    analyzer.generate_analysis_report()

if __name__ == "__main__":
    main()


