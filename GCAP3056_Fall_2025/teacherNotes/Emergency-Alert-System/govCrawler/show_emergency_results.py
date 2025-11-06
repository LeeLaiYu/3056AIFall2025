#!/usr/bin/env python3
"""
Show emergency-related URLs from the CSV
"""

import pandas as pd

def show_emergency_results():
    """Display URLs that contain emergency keywords"""
    
    df = pd.read_csv('emergency_directory_results_with_keywords.csv')
    
    # Filter rows with emergency keywords
    emergency_rows = df[df['has_emergency_keywords'] == True]
    
    print("URLs with Emergency Keywords:")
    print("=" * 60)
    print(f"Total emergency-related URLs found: {len(emergency_rows)}")
    print()
    
    for i, (idx, row) in enumerate(emergency_rows.iterrows(), 1):
        print(f"{i}. URL: {row['url']}")
        print(f"   Keywords: {row['emergency_keywords_found']}")
        print(f"   Title: {row['title'][:100]}...")
        print(f"   Crawled: {row['crawled_at']}")
        print()

if __name__ == "__main__":
    show_emergency_results()


