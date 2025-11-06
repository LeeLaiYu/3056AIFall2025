# HKO Web Crawler for Dr Tin Chatbot Analysis

This project contains web crawlers designed to crawl the Hong Kong Observatory (HKO) website and search for mentions of "Dr Tin chatbot" or related content.

## Overview

The crawler system consists of two main components:

1. **Basic HKO Crawler** - Simple web crawler with basic Dr Tin chatbot detection
2. **Enhanced HKO Crawler** - Advanced crawler with comprehensive content analysis and relevance scoring

## Features

### Basic Crawler (`hko_web_crawler.py`)
- Downloads all pages from HKO website
- Basic pattern matching for "Dr Tin chatbot" mentions
- Simple file organization
- CSV sitemap generation

### Enhanced Crawler (`enhanced_hko_crawler.py`)
- Advanced content analysis with multiple search patterns
- Relevance scoring system
- Comprehensive keyword analysis
- Organized folder structure for different content types
- Detailed reporting and logging

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start (Enhanced Crawler - Recommended)
```bash
python run_enhanced_crawler.py
```

### Basic Crawler
```bash
python run_hko_crawler.py
```

### Direct Execution
```bash
python enhanced_hko_crawler.py
python hko_web_crawler.py
```

## Output Structure

After running the crawler, the following folder structure will be created:

```
webCrawlHKO/
├── downloaded_pages/          # All downloaded HTML pages
├── dr_tin_mentions/          # Pages containing Dr Tin chatbot mentions
├── high_relevance_pages/     # Pages with high relevance scores (enhanced only)
├── reports/                  # Comprehensive analysis reports
│   ├── enhanced_hko_sitemap.csv
│   ├── dr_tin_mentions_detailed.md
│   ├── enhanced_crawl_summary.md
│   ├── content_analysis_report.md
│   └── content_analysis_data.json
├── logs/                     # Detailed crawler logs
│   └── enhanced_hko_crawler.log
├── webCrawllog_HKO.md        # Main log file
├── hko_web_crawler.py        # Basic crawler
├── enhanced_hko_crawler.py   # Enhanced crawler
├── content_analyzer.py       # Content analysis module
├── run_hko_crawler.py        # Basic crawler runner
├── run_enhanced_crawler.py   # Enhanced crawler runner
└── requirements.txt          # Dependencies
```

## Search Patterns

The enhanced crawler searches for various patterns related to Dr Tin chatbot:

### Direct Mentions
- "dr tin chatbot"
- "dr tin bot"
- "dr. tin chatbot"
- "dr. tin bot"

### Related Patterns
- "chatbot dr tin"
- "bot dr tin"
- "ai dr tin"
- "assistant dr tin"

### Weather-Related
- "weather chatbot"
- "weather bot"
- "hko chatbot"
- "observatory chatbot"

## Configuration

### Crawler Settings
- **Max Pages**: 500 (configurable)
- **Delay**: 1 second between requests
- **Timeout**: 30 seconds per request
- **User Agent**: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36

### Content Analysis Settings
- **Relevance Threshold**: 0.5 (for high relevance classification)
- **Context Window**: 150 characters around matches
- **Confidence Scoring**: Based on pattern matching and context

## Reports Generated

### 1. Enhanced Sitemap (CSV)
Complete sitemap with analysis data including:
- URL and filename
- Status and links found
- Dr Tin mention flags
- Relevance scores
- Keyword counts

### 2. Dr Tin Mentions Report (Markdown)
Detailed report of all pages containing Dr Tin chatbot mentions with:
- Full context around matches
- Confidence scores
- Pattern information

### 3. Content Analysis Report (Markdown)
Comprehensive analysis including:
- Summary statistics
- Keyword frequency analysis
- Relevance scoring breakdown

### 4. Crawl Summary (Markdown)
Overall crawl results and file organization

## Logging

The crawler provides comprehensive logging:

- **Main Log**: `webCrawllog_HKO.md` - High-level crawl results
- **Detailed Log**: `logs/enhanced_hko_crawler.log` - Detailed execution logs
- **Content Analysis Log**: `content_analyzer.log` - Analysis-specific logs

## Error Handling

The crawler includes robust error handling:
- Network timeout handling
- Invalid URL filtering
- File system error recovery
- Graceful interruption handling (Ctrl+C)

## Performance

- **Concurrent Processing**: Single-threaded with configurable delays
- **Memory Usage**: Efficient processing of large websites
- **Storage**: Organized file structure to prevent clutter
- **Resume Capability**: Can be interrupted and resumed

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed
2. **Permission Errors**: Check write permissions for output directory
3. **Network Timeouts**: Increase timeout values in crawler settings
4. **Memory Issues**: Reduce max_pages parameter for large sites

### Debug Mode

Enable detailed logging by modifying the logging level in the crawler:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Contributing

To extend the crawler:

1. **Add New Patterns**: Modify the `dr_tin_patterns` list in `content_analyzer.py`
2. **Custom Analysis**: Extend the `HKOContentAnalyzer` class
3. **New Output Formats**: Add new report generators in the crawler classes

## License

This project is part of the GCAP3056 course work for Hong Kong Observatory chatbot analysis.

## Contact

For questions or issues, refer to the course documentation or contact the development team.


