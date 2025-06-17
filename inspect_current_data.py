
#!/usr/bin/env python3
"""
Inspect current attribution data to see what email content we have
"""

import pandas as pd
import os

def inspect_current_data():
    """Inspect what email content data we currently have"""
    
    # Check the attribution file
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"Attribution file not found: {attribution_file}")
        return
    
    # Load the data
    df = pd.read_csv(attribution_file)
    print(f"Loaded {len(df)} leads from {attribution_file}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Check original_reason column specifically
    if 'original_reason' in df.columns:
        print("=== ORIGINAL_REASON COLUMN ANALYSIS ===")
        non_empty_reasons = df[df['original_reason'].notna() & (df['original_reason'] != '')]
        print(f"Leads with original_reason: {len(non_empty_reasons)}/{len(df)}")
        
        # Show first 10 examples of original_reason
        print("\nSample original_reason values:")
        for i, (idx, row) in enumerate(non_empty_reasons.head(10).iterrows()):
            print(f"\n{i+1}. Email: {row['email']}")
            print(f"   Original Reason: {row['original_reason'][:300]}...")
            if len(str(row['original_reason'])) > 300:
                print(f"   (truncated from {len(str(row['original_reason']))} chars)")
    else:
        print("No 'original_reason' column found")
    
    # Check for any columns that might contain email body content
    content_keywords = ['body', 'content', 'conversation', 'message', 'text']
    potential_content_cols = []
    
    for col in df.columns:
        if any(keyword in col.lower() for keyword in content_keywords):
            potential_content_cols.append(col)
    
    if potential_content_cols:
        print(f"\n=== POTENTIAL CONTENT COLUMNS ===")
        for col in potential_content_cols:
            non_empty = df[df[col].notna() & (df[col] != '')]
            print(f"Column '{col}': {len(non_empty)}/{len(df)} non-empty values")
            
            # Show sample
            if len(non_empty) > 0:
                sample = non_empty.iloc[0]
                print(f"  Sample: {str(sample)[:200]}...")
    else:
        print("\n=== NO OBVIOUS CONTENT COLUMNS FOUND ===")
    
    # Check ticket_subjects for patterns
    if 'ticket_subjects' in df.columns:
        print(f"\n=== TICKET_SUBJECTS ANALYSIS ===")
        subjects_with_content = df[df['ticket_subjects'].notna() & (df['ticket_subjects'] != '')]
        print(f"Leads with ticket subjects: {len(subjects_with_content)}/{len(df)}")
        
        # Look for specific patterns we need
        patterns_to_find = ['payment', 'hi darren', 'ordered', 'colleague', 'quotation']
        
        for pattern in patterns_to_find:
            pattern_matches = subjects_with_content[
                subjects_with_content['ticket_subjects'].str.contains(pattern, case=False, na=False)
            ]
            print(f"  Pattern '{pattern}': {len(pattern_matches)} matches")
            
            if len(pattern_matches) > 0:
                sample = pattern_matches.iloc[0]
                print(f"    Sample: {sample['ticket_subjects'][:200]}...")

if __name__ == "__main__":
    inspect_current_data()
