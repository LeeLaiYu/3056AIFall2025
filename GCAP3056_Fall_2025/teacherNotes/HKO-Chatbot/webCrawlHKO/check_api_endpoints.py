#!/usr/bin/env python3
"""
Check API Endpoints
Check the specific API endpoints found in the HTML analysis
"""

import requests
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIEndpointChecker:
    def __init__(self):
        self.base_url = "https://data.gov.hk"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Endpoints found in HTML analysis
        self.endpoints = {
            'datasets_api': '/api/v1/datasets',
            'providers_json': '/filestore/json/providers_en.json',
            'categories_json': '/filestore/json/categories_en.json',
            'formats_json': '/filestore/json/formats.json',
            'rss_feed': '/filestore/feeds/data_rss_en.xml',
            'climate_weather_category': '/en-datasets/category/climate-and-weather',
            'hko_provider': '/en-datasets/provider/hk-hko'
        }
    
    def check_endpoint(self, endpoint_name, endpoint_path):
        """Check a specific endpoint"""
        full_url = self.base_url + endpoint_path
        logger.info(f"Checking {endpoint_name}: {full_url}")
        
        try:
            response = self.session.get(full_url, timeout=30)
            logger.info(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '')
                logger.info(f"Content-Type: {content_type}")
                
                if 'application/json' in content_type:
                    try:
                        data = response.json()
                        logger.info(f"JSON data received: {len(str(data))} characters")
                        return {
                            'status': 'success',
                            'status_code': response.status_code,
                            'content_type': content_type,
                            'data': data,
                            'size': len(response.content)
                        }
                    except json.JSONDecodeError:
                        logger.warning("Response is not valid JSON")
                        return {
                            'status': 'success',
                            'status_code': response.status_code,
                            'content_type': content_type,
                            'data': response.text[:1000] + '...' if len(response.text) > 1000 else response.text,
                            'size': len(response.content)
                        }
                else:
                    logger.info(f"Non-JSON response: {len(response.text)} characters")
                    return {
                        'status': 'success',
                        'status_code': response.status_code,
                        'content_type': content_type,
                        'data': response.text[:1000] + '...' if len(response.text) > 1000 else response.text,
                        'size': len(response.content)
                    }
            else:
                logger.warning(f"HTTP {response.status_code}: {response.reason}")
                return {
                    'status': 'error',
                    'status_code': response.status_code,
                    'error': response.reason
                }
                
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def check_all_endpoints(self):
        """Check all endpoints"""
        logger.info("Checking all API endpoints...")
        
        results = {}
        
        for endpoint_name, endpoint_path in self.endpoints.items():
            result = self.check_endpoint(endpoint_name, endpoint_path)
            results[endpoint_name] = result
            
            # Add delay between requests
            import time
            time.sleep(1)
        
        return results
    
    def generate_report(self, results):
        """Generate a report of the findings"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate JSON report
        json_file = f"api_endpoints_check_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON report saved: {json_file}")
        
        # Generate markdown report
        md_file = f"api_endpoints_check_{timestamp}.md"
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# API Endpoints Check Report\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Summary\n\n")
            successful = sum(1 for r in results.values() if r.get('status') == 'success')
            f.write(f"- **Successful endpoints:** {successful}/{len(results)}\n")
            f.write(f"- **Failed endpoints:** {len(results) - successful}/{len(results)}\n\n")
            
            f.write("## Detailed Results\n\n")
            for endpoint_name, result in results.items():
                f.write(f"### {endpoint_name}\n")
                f.write(f"- **Status:** {result.get('status', 'unknown')}\n")
                f.write(f"- **HTTP Code:** {result.get('status_code', 'N/A')}\n")
                f.write(f"- **Content-Type:** {result.get('content_type', 'N/A')}\n")
                f.write(f"- **Size:** {result.get('size', 'N/A')} bytes\n")
                
                if result.get('status') == 'success':
                    if isinstance(result.get('data'), dict):
                        f.write(f"- **Data Type:** JSON object\n")
                        f.write(f"- **Keys:** {list(result['data'].keys()) if result['data'] else 'None'}\n")
                    else:
                        f.write(f"- **Data Type:** Text\n")
                        f.write(f"- **Preview:** {result['data'][:200]}...\n")
                else:
                    f.write(f"- **Error:** {result.get('error', 'Unknown error')}\n")
                
                f.write("\n")
            
            # Look for HKO-related data
            f.write("## HKO Dataset Analysis\n\n")
            hko_found = False
            
            for endpoint_name, result in results.items():
                if result.get('status') == 'success' and isinstance(result.get('data'), dict):
                    data = result['data']
                    if 'hko' in str(data).lower() or 'hong kong observatory' in str(data).lower():
                        f.write(f"### {endpoint_name} - HKO Data Found!\n")
                        f.write(f"```json\n{json.dumps(data, indent=2)[:1000]}...\n```\n\n")
                        hko_found = True
            
            if not hko_found:
                f.write("**No HKO datasets found in any of the checked endpoints.**\n\n")
                f.write("## Recommendations\n\n")
                f.write("1. **Contact HKO Directly**: The datasets may not be publicly available through data.gov.hk\n")
                f.write("2. **Check HKO Website**: Look for datasets on the HKO website directly\n")
                f.write("3. **Use RSS Feed**: Monitor the RSS feed for new HKO datasets\n")
                f.write("4. **Contact Support**: Email enquiry@1835500.gov.hk for dataset access\n")
        
        logger.info(f"Markdown report saved: {md_file}")
        return {'json_file': json_file, 'markdown_file': md_file}
    
    def run_check(self):
        """Run the complete check"""
        logger.info("Starting API endpoints check...")
        
        try:
            results = self.check_all_endpoints()
            report_files = self.generate_report(results)
            
            logger.info("API endpoints check completed!")
            logger.info(f"Report files generated: {report_files}")
            
            return {
                'results': results,
                'report_files': report_files
            }
            
        except Exception as e:
            logger.error(f"Error during check: {e}")
            return None

def main():
    """Main function to run the checker"""
    checker = APIEndpointChecker()
    result = checker.run_check()
    
    if result:
        print(f"\n✅ API endpoints check completed!")
        print(f"📊 Checked {len(result['results'])} endpoints")
        print(f"📁 Report files generated:")
        for file_type, file_path in result['report_files'].items():
            print(f"   - {file_type}: {file_path}")
    else:
        print("❌ API endpoints check failed. Check the logs for details.")

if __name__ == "__main__":
    main()


