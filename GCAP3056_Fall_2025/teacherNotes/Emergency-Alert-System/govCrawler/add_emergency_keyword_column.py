#!/usr/bin/env python3
"""
Add emergency keyword column to the CSV file
Identifies URLs that explicitly contain emergency-related keywords
"""

import pandas as pd
import re
import os

def add_emergency_keyword_column():
    """Add a column to identify URLs with explicit emergency keywords"""
    
    # Define emergency-related keywords
    emergency_keywords = [
        'emergency', 'crisis', 'disaster', 'alert', 'warning', 'preparedness',
        'response', 'rescue', 'safety', 'security', 'incident', 'hazard',
        'evacuation', 'shelter', 'relief', 'assistance', 'support', 'coordination',
        'management', 'control', 'monitoring', 'assessment', 'planning'
    ]
    
    # Read the CSV file
    csv_file = 'emergency_directory_results.csv'
    df = pd.read_csv(csv_file)
    
    def contains_emergency_keywords(url, title):
        """Check if URL or title contains emergency keywords"""
        text_to_check = f"{url} {title}".lower()
        
        # Check for exact keyword matches
        found_keywords = []
        for keyword in emergency_keywords:
            if keyword in text_to_check:
                found_keywords.append(keyword)
        
        return found_keywords if found_keywords else None
    
    # Add the new column
    df['emergency_keywords_found'] = df.apply(
        lambda row: contains_emergency_keywords(row['url'], row['title']), 
        axis=1
    )
    
    # Create a more readable version
    df['has_emergency_keywords'] = df['emergency_keywords_found'].notna()
    
    # Save the updated CSV
    output_file = 'emergency_directory_results_with_keywords.csv'
    df.to_csv(output_file, index=False)
    
    print(f"Updated CSV saved as: {output_file}")
    print(f"Total rows: {len(df)}")
    print(f"Rows with emergency keywords: {df['has_emergency_keywords'].sum()}")
    
    # Show some examples
    emergency_rows = df[df['has_emergency_keywords'] == True]
    if not emergency_rows.empty:
        print("\nExamples of URLs with emergency keywords:")
        for idx, row in emergency_rows.head(5).iterrows():
            print(f"- {row['url']}")
            print(f"  Keywords: {', '.join(row['emergency_keywords_found'])}")
            print(f"  Title: {row['title'][:100]}...")
            print()

if __name__ == "__main__":
    add_emergency_keyword_column()


