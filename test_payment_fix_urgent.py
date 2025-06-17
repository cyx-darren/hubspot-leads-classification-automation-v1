
#!/usr/bin/env python3
"""
Urgent test to verify payment email fixes
"""

import pandas as pd
import os
from modules.traffic_attribution import LeadAttributionAnalyzer

def test_payment_fix_urgent():
    """Test that the 5 critical payment emails are now being detected correctly"""
    
    # Load current attribution data
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"❌ Attribution file not found: {attribution_file}")
        return
    
    df = pd.read_csv(attribution_file)
    print(f"📊 Loaded {len(df)} leads from {attribution_file}")
    
    # Target emails that MUST be Direct
    target_emails = [
        'kezia.pranata@austenmaritime.com',
        'ser.j@nexus.edu.sg',
        'denise.velasco@dsv.com',
        'steven.riddle@icloud.com',
        'ap.singapore@aggreko.com'
    ]
    
    print("\n=== BEFORE FIX: Current Attribution ===")
    original_attributions = {}
    for email in target_emails:
        email_row = df[df['email'] == email]
        if not email_row.empty:
            current_source = email_row.iloc[0]['attributed_source']
            original_attributions[email] = current_source
            print(f"{email}: {current_source}")
        else:
            print(f"{email}: NOT FOUND")
            original_attributions[email] = "NOT FOUND"
    
    # Create analyzer and run email content analysis
    analyzer = LeadAttributionAnalyzer()
    analyzer.leads_df = df.copy()
    
    print("\n=== RUNNING FIXED EMAIL CONTENT ANALYSIS ===")
    analyzer.analyze_email_content_for_attribution_override()
    
    print("\n=== AFTER FIX: New Attribution ===")
    fixed_count = 0
    for email in target_emails:
        email_row = analyzer.leads_df[analyzer.leads_df['email'] == email]
        if not email_row.empty:
            new_source = email_row.iloc[0]['attributed_source']
            override_applied = email_row.iloc[0]['email_content_override']
            original = original_attributions[email]
            
            if new_source == 'Direct':
                print(f"✅ {email}: {original} → {new_source} (Override: {override_applied})")
                fixed_count += 1
            else:
                print(f"❌ {email}: {original} → {new_source} (Override: {override_applied}) - STILL NOT DIRECT!")
        else:
            print(f"❌ {email}: NOT FOUND")
    
    # Check for any payment content in ticket_subjects
    print("\n=== PAYMENT CONTENT ANALYSIS ===")
    payment_keywords = ['payment', 'remittance', 'invoice', 'outstanding', 'soa']
    
    for email in target_emails:
        email_row = analyzer.leads_df[analyzer.leads_df['email'] == email]
        if not email_row.empty:
            ticket_subjects = str(email_row.iloc[0].get('ticket_subjects', '')).lower()
            subject = str(email_row.iloc[0].get('subject', '')).lower()
            
            found_patterns = []
            for keyword in payment_keywords:
                if keyword in ticket_subjects:
                    found_patterns.append(f"ticket_subjects: {keyword}")
                if keyword in subject:
                    found_patterns.append(f"subject: {keyword}")
            
            if found_patterns:
                print(f"💰 {email}: Payment patterns found - {', '.join(found_patterns)}")
            else:
                print(f"⚠️  {email}: No payment patterns found")
    
    # Generate new attribution summary
    print("\n=== NEW ATTRIBUTION SUMMARY ===")
    new_summary = analyzer.leads_df['attributed_source'].value_counts()
    print("Attribution breakdown:")
    for source, count in new_summary.items():
        percentage = (count / len(analyzer.leads_df)) * 100
        print(f"  {source}: {count} leads ({percentage:.1f}%)")
    
    # Count total overrides
    total_overrides = analyzer.leads_df['email_content_override'].sum()
    print(f"\nTotal email content overrides applied: {total_overrides}")
    
    # Summary
    print(f"\n=== URGENT FIX RESULTS ===")
    print(f"Target emails fixed: {fixed_count}/5")
    print(f"Total payment overrides: {total_overrides}")
    
    if fixed_count == 5:
        print("✅ SUCCESS: All 5 target payment emails are now Direct!")
    else:
        print(f"❌ FAILURE: Only {fixed_count}/5 emails were fixed to Direct")
    
    # Save updated results
    output_path = 'output/leads_with_attribution_fixed.csv'
    analyzer.leads_df.to_csv(output_path, index=False)
    print(f"\n💾 Updated results saved to: {output_path}")
    
    return fixed_count == 5

if __name__ == "__main__":
    test_payment_fix_urgent()
