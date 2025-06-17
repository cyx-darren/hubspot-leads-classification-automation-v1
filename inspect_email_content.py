
#!/usr/bin/env python3
"""
Inspect email content data available in leads_with_attribution.csv
"""

import pandas as pd
import os

def inspect_email_content():
    """Inspect what email content data we have available"""
    
    # Check if the attribution file exists
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"Attribution file not found: {attribution_file}")
        return
    
    # Load the data
    df = pd.read_csv(attribution_file)
    print(f"Loaded {len(df)} leads from {attribution_file}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Check ticket_subjects column
    if 'ticket_subjects' in df.columns:
        print("=== TICKET SUBJECTS SAMPLE ===")
        non_empty_subjects = df[df['ticket_subjects'].notna() & (df['ticket_subjects'] != '')]
        print(f"Leads with ticket subjects: {len(non_empty_subjects)}/{len(df)}")
        
        # Show first 5 examples
        for i, (idx, row) in enumerate(non_empty_subjects.head(5).iterrows()):
            print(f"\n{i+1}. Email: {row['email']}")
            print(f"   Subjects: {row['ticket_subjects'][:200]}...")
            if 'products_mentioned' in df.columns:
                print(f"   Products: {row['products_mentioned']}")
    else:
        print("No 'ticket_subjects' column found")
    
    # Check if we have any conversation data
    conversation_cols = [col for col in df.columns if 'conversation' in col.lower() or 'content' in col.lower()]
    if conversation_cols:
        print(f"\n=== CONVERSATION COLUMNS FOUND ===")
        for col in conversation_cols:
            print(f"Column: {col}")
            non_empty = df[df[col].notna() & (df[col] != '')]
            print(f"Non-empty values: {len(non_empty)}/{len(df)}")
    else:
        print("\n=== NO CONVERSATION DATA FOUND ===")
        print("We need to fetch actual ticket conversations from Freshdesk")
    
    # Check for ticket IDs or other identifiable data
    id_cols = [col for col in df.columns if 'ticket' in col.lower() and ('id' in col.lower() or 'number' in col.lower())]
    print(f"\n=== TICKET ID COLUMNS ===")
    if id_cols:
        for col in id_cols:
            print(f"Column: {col}")
            non_empty = df[df[col].notna()]
            print(f"Non-empty values: {len(non_empty)}/{len(df)}")
    else:
        print("No explicit ticket ID columns found")
        print("We'll need to extract ticket IDs from the leads_with_products.csv or fetch them via email")

if __name__ == "__main__":
    inspect_email_content()
