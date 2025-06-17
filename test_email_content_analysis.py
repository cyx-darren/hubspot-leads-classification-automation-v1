
#!/usr/bin/env python3
"""
Test script to demonstrate email content analysis for attribution overrides
"""

import pandas as pd
import os
from modules.traffic_attribution import analyze_traffic_attribution

def create_test_data_with_email_content():
    """Create test data with realistic email content for testing"""
    
    test_leads = [
        {
            'email': 'sarah.marketing@techcorp.com',
            'attributed_source': 'SEO',
            'subject': "You've Got a New Enquiry! (Lanyard LP)",
            'conversation_snippets': "Hi, I'm interested in custom lanyards for our upcoming conference",
            'products_mentioned': 'Lanyards'
        },
        {
            'email': 'finance@globalltd.com', 
            'attributed_source': 'Referral',
            'subject': 'Payment inquiry',
            'conversation_snippets': "Hi, our payment is currently routing for approval. Please provide your latest SOA for our checking and payment processing.",
            'products_mentioned': ''
        },
        {
            'email': 'john.admin@startup.sg',
            'attributed_source': 'SEO', 
            'subject': 'Reorder request',
            'conversation_snippets': "Hi team, we have ordered business cards from you before and would like to reorder the same design. You still have our artwork from last time.",
            'products_mentioned': 'Business Cards'
        },
        {
            'email': 'mary.events@eventco.com',
            'attributed_source': 'PPC',
            'subject': 'New supplier inquiry', 
            'conversation_snippets': "Hi, I got your contact from my colleague Jessica who printed badges with you recently. She recommended your services.",
            'products_mentioned': 'Badges'
        },
        {
            'email': 'operations@logistics.com',
            'attributed_source': 'SEO',
            'subject': 'Badge printing inquiry',
            'conversation_snippets': "You've Got a New Enquiry! (Badge LP) - We need custom ID badges for our warehouse staff.",
            'products_mentioned': 'Badges'
        },
        {
            'email': 'accounts@retailchain.com',
            'attributed_source': 'Referral',
            'subject': 'Payment confirmation',
            'conversation_snippets': "Payment scheduled for invoice #INV-2024-001. The payment has been released and should reach you within 2 business days.",
            'products_mentioned': ''
        }
    ]
    
    return pd.DataFrame(test_leads)

def test_email_content_analysis():
    """Test the email content analysis functionality"""
    print("=" * 60)
    print("TESTING EMAIL CONTENT ANALYSIS")
    print("=" * 60)
    
    # Create test data
    test_df = create_test_data_with_email_content()
    
    # Save test data
    os.makedirs('output', exist_ok=True)
    test_path = 'output/test_leads_with_email_content.csv'
    test_df.to_csv(test_path, index=False)
    
    print(f"Created test data with {len(test_df)} leads containing email content")
    print("\nTest leads:")
    for idx, lead in test_df.iterrows():
        print(f"  {idx+1}. {lead['email']} - Original: {lead['attributed_source']}")
        print(f"     Subject: {lead['subject']}")
        print(f"     Content: {lead['conversation_snippets'][:80]}...")
        print()
    
    # Run attribution analysis
    try:
        from modules.traffic_attribution import LeadAttributionAnalyzer
        
        print("Running attribution analysis with email content analysis...")
        
        analyzer = LeadAttributionAnalyzer()
        analyzer.leads_df = test_df.copy()
        
        # Initialize required columns
        analyzer.leads_df['first_ticket_date'] = pd.Timestamp.now()
        analyzer.leads_df['first_inquiry_timestamp'] = pd.Timestamp.now()
        analyzer.leads_df['attributed_source'] = analyzer.leads_df['attributed_source']
        analyzer.leads_df['attribution_confidence'] = 75
        analyzer.leads_df['attribution_detail'] = 'Test attribution'
        analyzer.leads_df['data_source'] = 'test'
        
        # Initialize enhanced analysis columns
        analyzer.leads_df['drill_down'] = ''
        analyzer.leads_df['email_content_override'] = False
        analyzer.leads_df['override_reason'] = ''
        analyzer.leads_df['original_attributed_source'] = ''
        
        # Run email content analysis
        analyzer.analyze_email_content_for_attribution_override()
        
        # Show results
        print("\n" + "=" * 60)
        print("EMAIL CONTENT ANALYSIS RESULTS")
        print("=" * 60)
        
        for idx, lead in analyzer.leads_df.iterrows():
            email = lead['email']
            original = lead['original_attributed_source']
            new_source = lead['attributed_source']
            overridden = lead['email_content_override']
            
            if overridden:
                print(f"✓ OVERRIDE: {email}")
                print(f"  Original: {original} → New: {new_source}")
                print(f"  Drill-down: {lead['drill_down']}")
                print(f"  Reason: {lead['override_reason']}")
                print(f"  Confidence: {lead['attribution_confidence']}%")
            else:
                print(f"• No override: {email} - Remains {new_source}")
            print()
        
        # Save results
        result_path = 'output/test_results_email_analysis.csv'
        analyzer.leads_df.to_csv(result_path, index=False)
        print(f"Results saved to: {result_path}")
        
        return True
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_email_content_analysis()
    if success:
        print("\n✓ Email content analysis test completed successfully!")
    else:
        print("\n✗ Email content analysis test failed!")
