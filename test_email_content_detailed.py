
#!/usr/bin/env python3
"""
Test email content analysis with real Freshdesk ticket data
"""

import pandas as pd
import os
import sys
from datetime import datetime, timezone

# Add modules to path
sys.path.append('modules')

def test_email_content_analysis():
    """Test email content analysis on real data"""
    
    print("=== Testing Email Content Analysis ===\n")
    
    # Check if we have the required data
    leads_file = "output/leads_with_attribution.csv"
    if not os.path.exists(leads_file):
        print(f"Error: {leads_file} not found. Please run the main workflow first.")
        return
    
    # Load the data
    df = pd.read_csv(leads_file)
    print(f"Loaded {len(df)} leads from {leads_file}")
    
    # Show current attribution breakdown
    print("\n=== Current Attribution Breakdown ===")
    attribution_counts = df['attributed_source'].value_counts()
    for source, count in attribution_counts.items():
        print(f"  {source}: {count} leads ({count/len(df)*100:.1f}%)")
    
    # Test the attribution analyzer
    try:
        from traffic_attribution import LeadAttributionAnalyzer
        
        # Create analyzer instance
        analyzer = LeadAttributionAnalyzer()
        analyzer.leads_df = df.copy()
        
        print("\n=== Testing Email Content Fetching ===")
        
        # Test on a few sample emails
        test_emails = df['email'].head(3).tolist()
        
        for email in test_emails:
            print(f"\nTesting email: {email}")
            
            # Get ticket IDs
            ticket_ids = analyzer.get_ticket_ids_from_email(email)
            print(f"  Found {len(ticket_ids)} tickets: {ticket_ids}")
            
            if ticket_ids:
                # Test conversation fetching
                for ticket_id in ticket_ids[:1]:  # Test first ticket only
                    conversation = analyzer.fetch_ticket_conversations(ticket_id)
                    if conversation:
                        print(f"  Ticket {ticket_id} conversation preview:")
                        print(f"    {conversation[:200]}...")
                    else:
                        print(f"  Ticket {ticket_id}: No conversation text retrieved")
            else:
                print(f"  No tickets found for {email}")
        
        print("\n=== Running Full Email Content Analysis ===")
        
        # Store original attribution for comparison
        original_attribution = analyzer.leads_df['attributed_source'].copy()
        
        # Run the analysis
        analyzer.analyze_email_content_for_attribution_override()
        
        # Show changes
        changes = analyzer.leads_df['attributed_source'] != original_attribution
        changed_leads = analyzer.leads_df[changes]
        
        print(f"\n=== Attribution Changes ===")
        print(f"Total leads with attribution changes: {len(changed_leads)}")
        
        if len(changed_leads) > 0:
            print("\nDetailed changes:")
            for idx, lead in changed_leads.iterrows():
                original = original_attribution.iloc[idx]
                new = lead['attributed_source']
                reason = lead.get('override_reason', 'No reason provided')
                print(f"  {lead['email']}: {original} → {new}")
                print(f"    Reason: {reason}")
                if lead.get('drill_down'):
                    print(f"    Details: {lead['drill_down']}")
        
        # Show new attribution breakdown
        print("\n=== New Attribution Breakdown ===")
        new_attribution_counts = analyzer.leads_df['attributed_source'].value_counts()
        for source, count in new_attribution_counts.items():
            original_count = attribution_counts.get(source, 0)
            change = count - original_count
            change_str = f" ({change:+d})" if change != 0 else ""
            print(f"  {source}: {count} leads ({count/len(df)*100:.1f}%){change_str}")
        
        # Show sample content analysis results
        print("\n=== Sample Content Analysis Results ===")
        content_analyzed = analyzer.leads_df[analyzer.leads_df['drill_down'] != '']
        print(f"Leads with content analysis: {len(content_analyzed)}")
        
        for idx, lead in content_analyzed.head(5).iterrows():
            print(f"\n  {lead['email']}")
            print(f"    Attribution: {lead['attributed_source']}")
            print(f"    Override: {lead.get('email_content_override', False)}")
            print(f"    Drill down: {lead['drill_down']}")
            if lead.get('override_reason'):
                print(f"    Reason: {lead['override_reason']}")
        
    except ImportError as e:
        print(f"Error importing traffic attribution module: {e}")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_email_content_analysis()
