
#!/usr/bin/env python3
"""
Check which leads have payment-related content in ticket_subjects
"""

import pandas as pd
import re
import os

def check_payment_leads():
    """Check for payment-related content in ticket_subjects"""
    
    # Load attribution data
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"Attribution file not found: {attribution_file}")
        return
    
    df = pd.read_csv(attribution_file)
    print(f"Loaded {len(df)} leads from {attribution_file}")
    
    # Check ticket_subjects column for payment-related content
    if 'ticket_subjects' not in df.columns:
        print("No 'ticket_subjects' column found")
        return
    
    # Payment-related patterns
    payment_keywords = ['payment', 'remittance', 'invoice', 'soa', 'outstanding']
    
    print("\n=== LEADS WITH PAYMENT-RELATED TICKET SUBJECTS ===")
    
    found_payment_leads = 0
    
    for idx, row in df.iterrows():
        email = row['email']
        ticket_subjects = str(row.get('ticket_subjects', '')).lower()
        attributed_source = row.get('attributed_source', '')
        
        # Check for any payment-related keywords
        found_keywords = []
        for keyword in payment_keywords:
            if keyword in ticket_subjects:
                found_keywords.append(keyword)
        
        if found_keywords:
            found_payment_leads += 1
            print(f"\n{found_payment_leads}. Email: {email}")
            print(f"   Current Attribution: {attributed_source}")
            print(f"   Payment Keywords Found: {', '.join(found_keywords)}")
            print(f"   Ticket Subjects: {ticket_subjects[:200]}...")
            
            # Specifically check for 'Payment released - Easyprint'
            if 'payment released - easyprint' in ticket_subjects:
                print(f"   *** FOUND 'Payment released - Easyprint' ***")
            
            # Check if it should be Direct
            if attributed_source != 'Direct':
                print(f"   >>> SHOULD BE DIRECT (currently {attributed_source})")
    
    print(f"\n=== SUMMARY ===")
    print(f"Total leads with payment-related ticket subjects: {found_payment_leads}")
    
    # Show specific search for "Payment released - Easyprint"
    specific_pattern = df['ticket_subjects'].str.contains('Payment released - Easyprint', case=False, na=False)
    specific_matches = df[specific_pattern]
    
    print(f"\nLeads with 'Payment released - Easyprint': {len(specific_matches)}")
    for idx, row in specific_matches.iterrows():
        print(f"  - {row['email']}: {row['attributed_source']}")

if __name__ == "__main__":
    check_payment_leads()
