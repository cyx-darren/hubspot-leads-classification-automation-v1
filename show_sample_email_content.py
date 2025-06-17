
#!/usr/bin/env python3
"""
Show sample email content from current leads data
"""

import pandas as pd
import os

def show_sample_content():
    """Show sample email content from leads data"""
    
    print("=== Sample Email Content Inspection ===\n")
    
    # Check leads_with_products.csv first
    products_file = "output/leads_with_products.csv"
    if os.path.exists(products_file):
        df = pd.read_csv(products_file)
        print(f"Loaded {len(df)} leads from {products_file}")
        print(f"Columns: {list(df.columns)}\n")
        
        # Show ticket subjects samples
        if 'ticket_subjects' in df.columns:
            print("=== TICKET SUBJECTS SAMPLES ===")
            subjects_data = df[df['ticket_subjects'].notna() & (df['ticket_subjects'] != '')]
            print(f"Leads with subjects: {len(subjects_data)}/{len(df)}\n")
            
            for i, (idx, row) in enumerate(subjects_data.head(5).iterrows()):
                email = row['email']
                subjects = str(row['ticket_subjects'])
                products = row.get('products_mentioned', 'None')
                
                print(f"{i+1}. Email: {email}")
                print(f"   Products: {products}")
                print(f"   Subjects: {subjects}")
                print()
        
        # Check attribution file if it exists
        attr_file = "output/leads_with_attribution.csv"
        if os.path.exists(attr_file):
            attr_df = pd.read_csv(attr_file)
            print(f"\n=== ATTRIBUTION DATA COLUMNS ===")
            print(f"Attribution file columns: {list(attr_df.columns)}")
            
            # Check if we have the new override columns
            override_cols = ['drill_down', 'email_content_override', 'override_reason', 'original_attributed_source']
            existing_override_cols = [col for col in override_cols if col in attr_df.columns]
            print(f"Override columns present: {existing_override_cols}")
            
            if existing_override_cols:
                print(f"\n=== OVERRIDE STATUS ===")
                if 'email_content_override' in attr_df.columns:
                    override_count = attr_df['email_content_override'].sum() if 'email_content_override' in attr_df.columns else 0
                    print(f"Leads with content overrides: {override_count}")
                
                # Show sample overrides
                if 'drill_down' in attr_df.columns:
                    drill_downs = attr_df[attr_df['drill_down'].notna() & (attr_df['drill_down'] != '')]
                    print(f"Leads with drill-down info: {len(drill_downs)}")
                    
                    for i, (idx, row) in enumerate(drill_downs.head(3).iterrows()):
                        print(f"\n{i+1}. {row['email']}")
                        print(f"   Attribution: {row.get('attributed_source', 'Unknown')}")
                        print(f"   Drill down: {row['drill_down']}")
                        if 'override_reason' in row and pd.notna(row['override_reason']):
                            print(f"   Override reason: {row['override_reason']}")
    else:
        print(f"Products file not found: {products_file}")

if __name__ == "__main__":
    show_sample_content()
