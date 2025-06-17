
import pandas as pd
import os
from modules.traffic_attribution import LeadAttributionAnalyzer

def test_email_content_analysis():
    """Test email content analysis with sample data"""
    print("Testing email content analysis...")
    
    # Create test data with email content
    test_data = {
        'email': [
            'test1@example.com',
            'test2@example.com', 
            'test3@example.com',
            'test4@example.com'
        ],
        'attributed_source': ['Unknown', 'Unknown', 'Unknown', 'Unknown'],
        'attribution_confidence': [0, 0, 0, 0],
        'attribution_detail': ['', '', '', ''],
        'data_source': ['unknown', 'unknown', 'unknown', 'unknown'],
        'ticket_subjects': [
            "You've Got a New Enquiry! (Lanyard LP)",
            'Payment has been released for your invoice',
            'Hi John, got your contact from my colleague Sarah',
            'Regular inquiry about business cards'
        ],
        'conversation_snippets': [
            'Customer inquiry from landing page',
            'We need to provide our latest SOA for checking and payment',
            'My colleague Sarah shared your details with me',
            'Looking for custom business cards for our company'
        ],
        'first_inquiry_timestamp': [pd.Timestamp.now()] * 4,
        'first_ticket_date': [pd.Timestamp.now()] * 4
    }
    
    test_df = pd.DataFrame(test_data)
    
    # Initialize analyzer
    analyzer = LeadAttributionAnalyzer()
    analyzer.leads_df = test_df
    
    print("Before email content analysis:")
    for idx, row in test_df.iterrows():
        print(f"  {row['email']}: {row['attributed_source']}")
    
    # Run email content analysis
    analyzer.analyze_email_content_for_attribution_override()
    
    print("\nAfter email content analysis:")
    for idx, row in analyzer.leads_df.iterrows():
        override = row.get('email_content_override', False)
        reason = row.get('override_reason', '')
        drill_down = row.get('drill_down', '')
        print(f"  {row['email']}: {row['attributed_source']} (Override: {override})")
        if reason:
            print(f"    Reason: {reason}")
        if drill_down:
            print(f"    Drill-down: {drill_down}")
    
    # Check if new columns were added
    expected_columns = ['drill_down', 'email_content_override', 'override_reason', 'original_attributed_source']
    missing_columns = [col for col in expected_columns if col not in analyzer.leads_df.columns]
    
    if missing_columns:
        print(f"\n❌ Missing columns: {missing_columns}")
        return False
    else:
        print(f"\n✅ All expected columns present: {expected_columns}")
        
    # Check if any overrides were applied
    overrides = analyzer.leads_df[analyzer.leads_df['email_content_override'] == True]
    print(f"\n✅ Applied {len(overrides)} email content overrides")
    
    return True

if __name__ == "__main__":
    success = test_email_content_analysis()
    if success:
        print("\n✅ Email content analysis test passed!")
    else:
        print("\n❌ Email content analysis test failed!")
