
#!/usr/bin/env python3
"""
Test the fixed payment detection in email content analysis
"""

import pandas as pd
import os
import sys
sys.path.append('modules')

def test_payment_fix():
    """Test that payment patterns are now detected in ticket_subjects"""
    
    # Load attribution data
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"Attribution file not found: {attribution_file}")
        return
    
    df = pd.read_csv(attribution_file)
    print(f"Loaded {len(df)} leads for testing")
    
    # Find leads with payment-related ticket_subjects
    payment_leads = df[df['ticket_subjects'].str.contains('payment|remittance|invoice', case=False, na=False)]
    
    if len(payment_leads) == 0:
        print("No leads with payment-related ticket subjects found")
        return
    
    print(f"\nFound {len(payment_leads)} leads with payment-related ticket subjects")
    
    # Test with a small sample
    test_df = payment_leads.head(5).copy()
    
    try:
        from traffic_attribution import LeadAttributionAnalyzer
        
        # Create analyzer
        analyzer = LeadAttributionAnalyzer()
        analyzer.leads_df = test_df
        
        # Store original attributions
        original_attributions = test_df['attributed_source'].copy()
        
        print("\n=== BEFORE ANALYSIS ===")
        for idx, row in test_df.iterrows():
            email = row['email']
            source = row['attributed_source']
            subjects = str(row['ticket_subjects'])[:100]
            print(f"{email}: {source}")
            print(f"  Subjects: {subjects}...")
        
        # Run email content analysis
        print("\n=== RUNNING EMAIL CONTENT ANALYSIS ===")
        analyzer.analyze_email_content_for_attribution_override()
        
        print("\n=== AFTER ANALYSIS ===")
        changes_made = 0
        for idx, row in analyzer.leads_df.iterrows():
            email = row['email']
            new_source = row['attributed_source']
            override = row.get('email_content_override', False)
            reason = row.get('override_reason', '')
            
            original_idx = test_df.index[test_df['email'] == email].tolist()
            if original_idx:
                original_source = original_attributions.iloc[original_idx[0]]
                
                if new_source != original_source:
                    changes_made += 1
                    print(f"✓ CHANGED: {email}")
                    print(f"  {original_source} → {new_source}")
                    print(f"  Override: {override}")
                    print(f"  Reason: {reason}")
                else:
                    print(f"  No change: {email} - {new_source}")
        
        print(f"\n=== RESULTS ===")
        print(f"Total attribution changes: {changes_made}")
        
        if changes_made == 0:
            print("❌ No payment leads were changed to Direct - the fix may not be working")
        else:
            print("✅ Payment leads successfully detected and changed to Direct")
            
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_payment_fix()
