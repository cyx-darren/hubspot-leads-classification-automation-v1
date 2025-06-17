
#!/usr/bin/env python3
"""
Test enhanced email content analysis with Freshdesk API fetching
"""

import pandas as pd
import os
import sys
from datetime import datetime, timezone

# Add modules to path
sys.path.append('modules')

def test_enhanced_email_analysis():
    """Test the enhanced email content analysis with API fetching"""
    
    # Check if attribution file exists
    attribution_file = "output/leads_with_attribution.csv"
    if not os.path.exists(attribution_file):
        print(f"Attribution file not found: {attribution_file}")
        print("Please run main.py first to generate the attribution file")
        return
    
    # Load a small sample for testing
    df = pd.read_csv(attribution_file)
    print(f"Loaded {len(df)} leads from attribution file")
    
    # Test with first 5 leads
    test_df = df.head(5).copy()
    print(f"Testing with {len(test_df)} leads")
    
    # Import and test the traffic attribution analyzer
    try:
        from modules.traffic_attribution import LeadAttributionAnalyzer
        
        # Create analyzer instance
        analyzer = LeadAttributionAnalyzer()
        analyzer.leads_df = test_df
        
        print("\n=== Testing Enhanced Email Content Analysis ===")
        print("This will fetch actual conversations from Freshdesk API...")
        
        # Test the enhanced email content analysis
        analyzer.analyze_email_content_for_attribution_override()
        
        # Show results
        print("\n=== RESULTS ===")
        for idx, row in analyzer.leads_df.iterrows():
            email = row['email']
            override = row.get('email_content_override', False)
            reason = row.get('override_reason', '')
            source = row.get('attributed_source', '')
            
            if override:
                print(f"✓ {email}: {source} - {reason}")
            else:
                print(f"  {email}: No override")
        
        print("\n=== Test completed ===")
        
    except ImportError as e:
        print(f"Error importing traffic attribution module: {e}")
    except Exception as e:
        print(f"Error during testing: {e}")

if __name__ == "__main__":
    test_enhanced_email_analysis()
