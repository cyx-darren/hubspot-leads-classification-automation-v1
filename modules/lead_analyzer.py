
import pandas as pd
import requests
import json
import os
import sys
from datetime import datetime, timezone
import time
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import re
from typing import List, Dict, Optional, Tuple
import calendar

# Load environment variables
load_dotenv()

# API configuration
FRESHDESK_API_KEY = os.environ.get('FRESHDESK_API_KEY')
FRESHDESK_DOMAIN = os.environ.get('FRESHDESK_DOMAIN')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')  # Optional for AI product detection

# Color codes for better terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_colored(text: str, color: str):
    """Print text with color for better readability"""
    print(f"{color}{text}{Colors.ENDC}")

def load_product_catalog(file_path="./data/Product_Catalogue.csv"):
    """Load the product catalog from CSV file"""
    try:
        df = pd.read_csv(file_path)
        print_colored(f"✓ Loaded product catalog with {len(df)} products", Colors.GREEN)
        return df
    except FileNotFoundError:
        print_colored(f"Error: Product catalog not found at {file_path}", Colors.RED)
        return pd.DataFrame()
    except Exception as e:
        print_colored(f"Error loading product catalog: {e}", Colors.RED)
        return pd.DataFrame()

def get_ticket_conversations(ticket_id: int) -> List[Dict]:
    """Get all conversations for a specific ticket"""
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        return []
    
    url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets/{ticket_id}/conversations"
    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.get(url, headers=headers, auth=auth)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            print_colored(f"Rate limited. Waiting {retry_after} seconds...", Colors.YELLOW)
            time.sleep(retry_after)
            return get_ticket_conversations(ticket_id)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print_colored(f"Error getting conversations for ticket {ticket_id}: {e}", Colors.RED)
        return []

def parse_ticket_date(date_str: str) -> datetime:
    """Parse Freshdesk date string to datetime object."""
    try:
        # Freshdesk typically returns dates in ISO format
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        try:
            # Fallback to other common formats
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
        except:
            return None

def is_ticket_in_analysis_period(ticket: Dict, start_date: datetime, end_date: datetime) -> bool:
    """Check if a ticket was created within the specified analysis period."""
    created_at = ticket.get('created_at')
    if not created_at:
        return False

    ticket_date = parse_ticket_date(created_at)
    if not ticket_date:
        return False

    # Ensure ticket_date is timezone-aware
    if ticket_date.tzinfo is None:
        ticket_date = ticket_date.replace(tzinfo=timezone.utc)

    return start_date <= ticket_date <= end_date

def get_tickets_for_email_in_period(email: str, start_date: datetime, end_date: datetime) -> List[Dict]:
    """Get all tickets for an email within a specific date range using the same approach as spam_detector"""
    print(f"  DEBUG: Fetching tickets for {email}...")
    print(f"  DEBUG: Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print_colored("  DEBUG: Missing API credentials!", Colors.RED)
        return []
    
    print(f"  DEBUG: Using domain: {FRESHDESK_DOMAIN}")
    
    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}
    all_tickets = []
    
    try:
        # Approach 1: Try search query with spaces around AND operators
        search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
        print(f"  DEBUG: Search URL: {search_url}")
        
        # Format dates for Freshdesk query with proper spacing
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        query = f"email:'{email}' AND created_at:>'{start_str}' AND created_at:<'{end_str}'"
        params = {"query": query}
        print(f"  DEBUG: Query with proper spacing: {query}")
        
        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Search response status: {response.status_code}")
        
        if response.status_code == 200:
            tickets = response.json()
            print(f"  DEBUG: Found {len(tickets)} tickets from search")
            if tickets:
                # Filter tickets to ensure they're in date range
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                print(f"  DEBUG: {len(filtered_tickets)} tickets after date filtering")
                return filtered_tickets
        else:
            print(f"  DEBUG: Search with date filter failed: {response.text}")
        
        # Approach 2: Get all tickets for email using search (no date filter)
        print("  DEBUG: Trying search without date filter...")
        params = {"query": f"email:'{email}'"}
        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Search response status: {response.status_code}")
        
        if response.status_code == 200:
            tickets = response.json()
            print(f"  DEBUG: Found {len(tickets)} total tickets for email")
            if tickets:
                # Filter by date range using our date checking function
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                print(f"  DEBUG: {len(filtered_tickets)} tickets after date filtering")
                all_tickets.extend(filtered_tickets)
                return all_tickets
        else:
            print(f"  DEBUG: Search without date filter failed: {response.text}")
        
        # Approach 3: Use direct filter query like spam_detector (most reliable)
        print("  DEBUG: Trying direct filter approach like spam_detector...")
        filter_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets"
        params = {"email": email}
        print(f"  DEBUG: Filter URL: {filter_url}")
        
        response = requests.get(filter_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Filter response status: {response.status_code}")
        
        if response.status_code == 200:
            tickets = response.json()
            print(f"  DEBUG: Found {len(tickets)} total tickets for email using filter")
            if tickets:
                # Filter by date range using our date checking function
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                print(f"  DEBUG: {len(filtered_tickets)} tickets after date filtering")
                all_tickets.extend(filtered_tickets)
                return all_tickets
        else:
            print(f"  DEBUG: Filter approach failed: {response.text}")
        
        print("  DEBUG: No tickets found with any approach")
        return []
        
    except Exception as e:
        print_colored(f"  DEBUG: Exception getting tickets for {email}: {e}", Colors.RED)
        return []

def extract_product_mentions(text: str, product_catalog: pd.DataFrame) -> List[str]:
    """Extract product mentions from text using product catalog"""
    if product_catalog.empty:
        return []
    
    text_lower = text.lower()
    mentioned_products = []
    
    # Check for exact product name matches
    for _, product in product_catalog.iterrows():
        product_name = str(product.get('Product Name', '')).lower()
        category = str(product.get('Category', '')).lower()
        subcategory = str(product.get('Subcategory', '')).lower()
        
        # Check for product name mentions
        if product_name and len(product_name) > 3 and product_name in text_lower:
            mentioned_products.append(product_name.title())
        
        # Check for category mentions
        if category and len(category) > 3 and category in text_lower:
            mentioned_products.append(f"Category: {category.title()}")
        
        # Check for subcategory mentions
        if subcategory and len(subcategory) > 3 and subcategory in text_lower:
            mentioned_products.append(f"Subcategory: {subcategory.title()}")
    
    return list(set(mentioned_products))  # Remove duplicates

def analyze_lead_products(email: str, product_catalog: pd.DataFrame, start_date: datetime, end_date: datetime) -> Dict:
    """Analyze a lead's product interests based on Freshdesk tickets"""
    result = {
        'email': email,
        'total_tickets': 0,
        'product_mentions': [],
        'ticket_subjects': [],
        'conversation_snippets': [],
        'analysis_period': f"{start_date.strftime('%B %Y')} - {end_date.strftime('%B %Y')}"
    }
    
    print(f"  DEBUG: Starting analysis for {email}")
    
    # Get tickets for the specified period
    tickets = get_tickets_for_email_in_period(email, start_date, end_date)
    result['total_tickets'] = len(tickets)
    
    print(f"  DEBUG: Lead analyzer found {len(tickets)} tickets for {email}")
    
    if not tickets:
        return result
    
    all_product_mentions = []
    
    for ticket in tickets:
        ticket_id = ticket.get('id')
        subject = ticket.get('subject', '')
        description = ticket.get('description_text', '')
        
        result['ticket_subjects'].append(subject)
        
        # Extract products from ticket subject and description
        ticket_text = f"{subject} {description}".lower()
        products_in_ticket = extract_product_mentions(ticket_text, product_catalog)
        all_product_mentions.extend(products_in_ticket)
        
        # Get conversations for this ticket
        conversations = get_ticket_conversations(ticket_id)
        
        for conv in conversations:
            if isinstance(conv, dict):
                body_text = conv.get('body_text', '')
                if body_text:
                    # Store snippet for analysis
                    snippet = body_text[:200] + "..." if len(body_text) > 200 else body_text
                    result['conversation_snippets'].append(snippet)
                    
                    # Extract products from conversation
                    conv_products = extract_product_mentions(body_text, product_catalog)
                    all_product_mentions.extend(conv_products)
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)
    
    # Remove duplicates and sort
    result['product_mentions'] = sorted(list(set(all_product_mentions)))
    
    return result

def analyze_leads(input_csv_path="./output/not_spam_leads.csv", 
                 output_csv_path="./output/leads_with_products.csv",
                 start_date=None, end_date=None):
    """Main function to analyze leads and their product interests"""
    
    # Default to March-May 2025 if no dates provided
    if not start_date:
        start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    if not end_date:
        end_date = datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
    
    print_colored(f"Starting lead analysis for period: {start_date.strftime('%B %d, %Y')} - {end_date.strftime('%B %d, %Y')}", Colors.BLUE)
    
    # Check if API credentials are set
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print_colored("Error: Freshdesk API credentials not set", Colors.RED)
        return False
    
    # Load input CSV
    try:
        leads_df = pd.read_csv(input_csv_path)
        print_colored(f"✓ Loaded {len(leads_df)} leads from {input_csv_path}", Colors.GREEN)
    except FileNotFoundError:
        print_colored(f"Error: Input file not found: {input_csv_path}", Colors.RED)
        return False
    except Exception as e:
        print_colored(f"Error loading input file: {e}", Colors.RED)
        return False
    
    # Load product catalog
    product_catalog = load_product_catalog()
    if product_catalog.empty:
        print_colored("Warning: No product catalog loaded. Product analysis will be limited.", Colors.YELLOW)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    
    # Analyze each lead
    analyzed_leads = []
    
    for index, row in leads_df.iterrows():
        email = row.get('email', '')
        if not email:
            continue
        
        progress = f"[{index + 1}/{len(leads_df)}]"
        print(f"\n{progress} Analyzing: {email}")
        
        try:
            analysis = analyze_lead_products(email, product_catalog, start_date, end_date)
            
            # Create output row
            output_row = {
                'email': email,
                'original_classification': row.get('classification', ''),
                'original_reason': row.get('reason', ''),
                'total_tickets_analyzed': analysis['total_tickets'],
                'products_mentioned': '; '.join(analysis['product_mentions']),
                'ticket_subjects': '; '.join(analysis['ticket_subjects']),
                'analysis_period': analysis['analysis_period']
            }
            
            analyzed_leads.append(output_row)
            
            # Show progress
            product_count = len(analysis['product_mentions'])
            if product_count > 0:
                print_colored(f"{progress} Found {product_count} product mentions in {analysis['total_tickets']} tickets", Colors.GREEN)
            else:
                print_colored(f"{progress} No product mentions found in {analysis['total_tickets']} tickets", Colors.YELLOW)
        
        except Exception as e:
            print_colored(f"{progress} Error analyzing {email}: {e}", Colors.RED)
            # Add error row
            output_row = {
                'email': email,
                'original_classification': row.get('classification', ''),
                'original_reason': row.get('reason', ''),
                'total_tickets_analyzed': 0,
                'products_mentioned': f'Error: {str(e)}',
                'ticket_subjects': '',
                'analysis_period': f"{start_date.strftime('%B %Y')} - {end_date.strftime('%B %Y')}"
            }
            analyzed_leads.append(output_row)
    
    # Save results
    try:
        results_df = pd.DataFrame(analyzed_leads)
        results_df.to_csv(output_csv_path, index=False)
        print_colored(f"\n✓ Analysis complete! Results saved to {output_csv_path}", Colors.GREEN)
        
        # Summary statistics
        total_leads = len(analyzed_leads)
        leads_with_products = len([l for l in analyzed_leads if l['products_mentioned'] and not l['products_mentioned'].startswith('Error:')])
        leads_with_tickets = len([l for l in analyzed_leads if l['total_tickets_analyzed'] > 0])
        
        print_colored(f"\n=== Lead Analysis Summary ===", Colors.BOLD + Colors.BLUE)
        print(f"Total leads analyzed: {total_leads}")
        print(f"Leads with tickets in period: {leads_with_tickets}")
        print(f"Leads with product mentions: {leads_with_products}")
        
        if total_leads > 0:
            print(f"Percentage with product data: {(leads_with_products/total_leads)*100:.1f}%")
        
        return True
        
    except Exception as e:
        print_colored(f"Error saving results: {e}", Colors.RED)
        return False

def test_ticket_comparison(email: str, start_date: datetime, end_date: datetime):
    """Test function to compare lead analyzer vs spam detector ticket fetching"""
    print_colored(f"\n=== Testing ticket fetching for {email} ===", Colors.BOLD + Colors.BLUE)
    
    # Test lead analyzer approach
    print_colored("Testing Lead Analyzer approach:", Colors.BLUE)
    lead_tickets = get_tickets_for_email_in_period(email, start_date, end_date)
    print_colored(f"Lead analyzer found: {len(lead_tickets)} tickets", Colors.GREEN if lead_tickets else Colors.RED)
    
    # Test spam detector approach (import and use same function)
    try:
        from modules.spam_detector import SpamDetector
        detector = SpamDetector(start_date=start_date, end_date=end_date)
        print_colored("Testing Spam Detector approach:", Colors.BLUE)
        spam_tickets = detector.get_tickets_for_email(email)
        print_colored(f"Spam detector found: {len(spam_tickets)} tickets", Colors.GREEN if spam_tickets else Colors.RED)
        
        if spam_tickets:
            print("Sample ticket from spam detector:")
            print(f"  ID: {spam_tickets[0].get('id')}")
            print(f"  Subject: {spam_tickets[0].get('subject', 'N/A')}")
            print(f"  Created: {spam_tickets[0].get('created_at', 'N/A')}")
            
    except Exception as e:
        print_colored(f"Error testing spam detector: {e}", Colors.RED)

def main():
    """Standalone main function for testing"""
    # Test with a known email from the not_spam_leads.csv
    start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
    
    # Test with first few emails from the not_spam list
    test_emails = ["james.lim@outlook.com", "david@aeacorp.com.sg", "admin@sgcarstore.com"]
    
    for email in test_emails:
        test_ticket_comparison(email, start_date, end_date)
        print()
    
    return analyze_leads()

if __name__ == "__main__":
    main()
