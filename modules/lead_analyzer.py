
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
import csv
from fuzzywuzzy import fuzz, process

# Get the project root directory when running directly
if __name__ == "__main__":
    # If running directly, adjust path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    os.chdir(project_root)  # Change to project root

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
    products = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            
            # Check if first row looks like a header
            first_row = next(reader, None)
            if first_row:
                # If it contains actual product names (not "Product" or similar header)
                first_col = first_row[0].strip().lstrip('\ufeff') if first_row[0] else ""  # Handle BOM
                if first_col and not first_col.lower() in ['product', 'products', 'product name']:
                    # First row is a product, not a header
                    product_name = first_col
                    category = first_row[1].strip().lstrip('\ufeff') if len(first_row) > 1 else 'General'
                    products.append({'name': product_name, 'category': category})
                
                # Read remaining rows
                for row in reader:
                    if row and row[0].strip():  # If row exists and first column is not empty
                        product_name = row[0].strip().lstrip('\ufeff')  # Handle BOM
                        category = row[1].strip().lstrip('\ufeff') if len(row) > 1 else 'General'
                        products.append({'name': product_name, 'category': category})
        
        print_colored(f"✓ Loaded product catalog with {len(products)} products", Colors.GREEN)
        if products:
            print(f"DEBUG: First 5 products: {[p['name'] for p in products[:5]]}")
            
    except FileNotFoundError:
        print_colored(f"Error: Product catalog not found at {file_path}", Colors.RED)
        return []
    except Exception as e:
        print_colored(f"Error reading product catalog: {e}", Colors.RED)
        return []
    
    return products

def is_auto_generated_note(text):
    """Check if the text is an auto-generated staff note"""
    if not text:
        return False
        
    # Key phrases that identify auto-generated notes
    auto_note_indicators = [
        "A friendly reminder to all Sales Agents",
        "EASYPRINT is dedicated to providing the highest quality",
        "There are certain customers that we need to follow",
        "commonly asked questions",
        "When to give E-invoicing credit terms",
        "Sample Lead Time and Cost",
        "Lanyard Ordering Lead Time",
        "freshdesk.com/a/solutions/articles"
    ]
    
    # If text contains multiple indicators, it's likely auto-generated
    matches = sum(1 for indicator in auto_note_indicators if indicator in text)
    return matches >= 2

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
    """
    Get all tickets associated with an email from Freshdesk within the date range.
    Uses the EXACT same approach as spam_detector.py for consistency.
    """
    print(f"  DEBUG: Fetching tickets for {email}...")
    print(f"  DEBUG: Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print_colored("  DEBUG: Missing API credentials!", Colors.RED)
        return []
    
    print(f"  DEBUG: Using domain: {FRESHDESK_DOMAIN}")
    
    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}
    all_tickets = []

    # Approach 1: Try search query with date filters (EXACT copy from spam_detector)
    try:
        search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
        # Format dates for Freshdesk query
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        # Try with date range in query
        query = f"email:'{email}' AND created_at:>'{start_str}' AND created_at:<'{end_str}'"
        params = {"query": query}

        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Search with date filter response status: {response.status_code}")

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets to ensure they're in date range
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                all_tickets.extend(filtered_tickets)
                print(f"  DEBUG: Found {len(filtered_tickets)} tickets with date filter")
                return all_tickets
        else:
            print(f"  DEBUG: Search with date filter failed: {response.text}")
    except Exception as e:
        print_colored(f"  DEBUG: Search query with date filter approach failed: {e}", Colors.YELLOW)

    # Approach 2: Get all tickets for email and filter by date (EXACT copy from spam_detector)
    try:
        search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
        params = {"query": f"email:'{email}'"}

        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Search without date filter response status: {response.status_code}")

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets by date range
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                all_tickets.extend(filtered_tickets)
                print(f"  DEBUG: Found {len(filtered_tickets)} tickets after filtering")
                return all_tickets
        else:
            print(f"  DEBUG: Search without date filter failed: {response.text}")
    except Exception as e:
        print_colored(f"  DEBUG: Search query approach failed: {e}", Colors.YELLOW)

    # Approach 3: Try direct filter query (EXACT copy from spam_detector)
    try:
        filter_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets"
        params = {"email": email}

        response = requests.get(filter_url, headers=headers, auth=auth, params=params)
        print(f"  DEBUG: Direct filter response status: {response.status_code}")

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets by date range
                filtered_tickets = [t for t in tickets if is_ticket_in_analysis_period(t, start_date, end_date)]
                all_tickets.extend(filtered_tickets)
                print(f"  DEBUG: Found {len(filtered_tickets)} tickets with direct filter")
                return all_tickets
        else:
            print(f"  DEBUG: Direct filter failed: {response.text}")
    except Exception as e:
        print_colored(f"  DEBUG: Filter query approach failed: {e}", Colors.YELLOW)

    # If we've reached here, we couldn't get tickets using any approach
    if not all_tickets:
        print_colored(f"  DEBUG: Could not retrieve tickets for {email} using available API methods", Colors.YELLOW)

    return all_tickets

def detect_buying_intent(text: str) -> bool:
    """Detect if text contains buying intent phrases"""
    if not text:
        return False
    
    text_lower = text.lower()
    
    # Buying intent phrases
    intent_phrases = [
        'looking for', 'need quote', 'need quotation', 'interested in', 'want to order',
        'would like to', 'planning to buy', 'require', 'seeking', 'in search of',
        'can you provide', 'please quote', 'how much', 'price for', 'cost of',
        'bulk order', 'corporate gift', 'promotional', 'customize', 'custom',
        'inquiry', 'enquiry', 'request', 'rfq'
    ]
    
    return any(phrase in text_lower for phrase in intent_phrases)

def extract_quantities(text: str) -> List[str]:
    """Extract quantity mentions from text"""
    if not text:
        return []
    
    # Regex patterns for quantities
    quantity_patterns = [
        r'\b(\d{1,5})\s*(pieces?|pcs?|units?|items?)\b',
        r'\b(\d{1,5})\s*(dozen|hundreds?|thousands?)\b',
        r'\bquantity\s*:?\s*(\d{1,5})\b',
        r'\bqty\s*:?\s*(\d{1,5})\b',
        r'\b(\d{1,5})\s*sets?\b'
    ]
    
    quantities = []
    for pattern in quantity_patterns:
        matches = re.finditer(pattern, text.lower())
        for match in matches:
            quantity = match.group(1) if match.group(1).isdigit() else match.group(0)
            unit = match.group(2) if len(match.groups()) > 1 else ""
            quantities.append(f"{quantity} {unit}".strip())
    
    return list(set(quantities))  # Remove duplicates

def extract_product_mentions(text: str, product_catalog: List[Dict], is_subject=False) -> List[str]:
    """Extract product mentions from text with fuzzy matching and contextual understanding"""
    # Add null check at the start
    if not text:
        print("    DEBUG: Text is None or empty")
        return []
    
    if not product_catalog:
        print("    DEBUG: Product catalog is empty")
        return []
    
    # Generic terms to filter out
    GENERIC_TERMS = {
        'custom', 'singapore', 'delivery', 'printing', 'print', 'large', 'small', 
        'corporate', 'premium', 'standard', 'quality', 'design', 'service',
        'free', 'bulk', 'wholesale', 'retail', 'online', 'order', 'request'
    }
    
    # Safe to process now
    text_lower = text.lower()
    mentioned_products = []
    confidence_scores = []  # Track confidence for sorting
    
    print(f"    DEBUG: Analyzing {'SUBJECT' if is_subject else 'text'}: {text[:100]}...")
    
    # Check for buying intent
    has_buying_intent = detect_buying_intent(text)
    print(f"    DEBUG: Buying intent detected: {has_buying_intent}")
    
    # Extract quantities
    quantities = extract_quantities(text)
    if quantities:
        print(f"    DEBUG: Quantities found: {quantities}")
    
    # Create list of product names for fuzzy matching
    product_names = [p['name'] for p in product_catalog if p.get('name')]
    
    # Priority 1: Exact product name matches (highest confidence - 100)
    exact_matches = []
    for product_name in product_names:
        if product_name.lower() in text_lower:
            exact_matches.append(product_name)
            confidence_scores.append((product_name, 100, 'exact'))
            print(f"    DEBUG: Exact match found: '{product_name}' (confidence: 100)")
    
    mentioned_products.extend(exact_matches)
    
    # Priority 2: Fuzzy matching for typos and variations (75+ confidence)
    if len(mentioned_products) < 5:  # Only if we don't have too many already
        # Split text into words and check each potential product mention
        words = re.findall(r'\b\w+\b', text_lower)
        
        # Look for potential product phrases (1-4 words)
        for i in range(len(words)):
            for length in range(1, min(5, len(words) - i + 1)):
                phrase = ' '.join(words[i:i+length])
                
                # Skip if too short or is a generic term
                if len(phrase) < 3 or phrase in GENERIC_TERMS:
                    continue
                
                # Use fuzzy matching to find similar product names
                fuzzy_matches = process.extractBests(
                    phrase, 
                    product_names, 
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=75,
                    limit=3
                )
                
                for match_name, score in fuzzy_matches:
                    if match_name not in [p for p, _, _ in confidence_scores]:
                        mentioned_products.append(match_name)
                        confidence_scores.append((match_name, score, 'fuzzy'))
                        print(f"    DEBUG: Fuzzy match: '{phrase}' → '{match_name}' (confidence: {score})")
    
    # Priority 3: Multi-word product partial matches (60+ confidence)
    if len(mentioned_products) < 5:
        for product_name in product_names:
            if product_name in [p for p, _, _ in confidence_scores]:
                continue
            
            product_words = [word for word in product_name.lower().split() 
                           if len(word) > 3 and word not in GENERIC_TERMS]
            
            if len(product_words) >= 2:  # Only for multi-word products
                matches = sum(1 for word in product_words if word in text_lower)
                
                # Need at least 2 meaningful words to match
                if matches >= 2 and matches >= len(product_words) * 0.6:
                    confidence = int((matches / len(product_words)) * 80)
                    mentioned_products.append(product_name)
                    confidence_scores.append((product_name, confidence, 'partial'))
                    print(f"    DEBUG: Multi-word match: '{product_name}' (matched {matches}/{len(product_words)} words, confidence: {confidence})")
    
    # Priority 4: Product type indicators with special lanyard logic (50+ confidence)
    if len(mentioned_products) < 3:
        product_indicators = {
            'bag': ['bag', 'bags', 'tote', 'drawstring', 'mesh bag', 'paper bag', 'shopping bag'],
            'adapter': ['adapter', 'adapters', 'travel adapter', 'power adapter'],
            'apparel': ['shirt', 'polo', 't-shirt', 'hoodie', 'jacket', 'vest', 'apron'],
            'drinkware': ['mug', 'bottle', 'tumbler', 'flask', 'cup'],
            'accessories': ['keychain', 'umbrella', 'cap', 'hat'],  # Removed 'lanyard' for special handling
            'stationery': ['pen', 'notebook', 'folder', 'organizer', 'diary']
        }
        
        # Special handling for lanyard detection
        if 'lanyard' in text_lower:
            has_leather = 'leather' in text_lower
            has_keychain = 'keychain' in text_lower
            
            # Determine which lanyard product to use
            if has_leather and has_keychain:
                # If both leather and keychain, prefer leather lanyards (more specific)
                lanyard_product = "Leather Lanyards"
            elif has_leather:
                lanyard_product = "Leather Lanyards"
            elif has_keychain:
                lanyard_product = "Lanyard Keychain"
            else:
                # Just "lanyard" without leather or keychain
                lanyard_product = "Lanyards (With Printing)"
            
            # Check if this product hasn't been added yet
            if lanyard_product not in [prod for prod, _, _ in confidence_scores]:
                mentioned_products.append(lanyard_product)
                confidence_scores.append((lanyard_product, 60, 'lanyard_special'))
                print(f"    DEBUG: Special lanyard match: 'lanyard' → '{lanyard_product}' (confidence: 60)")
        
        # Handle other product indicators
        for category, keywords in product_indicators.items():
            for keyword in keywords:
                if keyword in text_lower and keyword not in GENERIC_TERMS:
                    # Check if this maps to an actual product in catalog
                    matching_products = [p for p in product_names 
                                       if keyword.lower() in p.lower() 
                                       and p not in [prod for prod, _, _ in confidence_scores]]
                    
                    if matching_products:
                        # Add the most specific match
                        best_match = min(matching_products, key=len)
                        mentioned_products.append(best_match)
                        confidence_scores.append((best_match, 50, 'indicator'))
                        print(f"    DEBUG: Product type match: '{keyword}' → '{best_match}' (confidence: 50)")
                        break
    
    # Sort by confidence (highest first)
    if confidence_scores:
        sorted_products = sorted(confidence_scores, key=lambda x: x[1], reverse=True)
        
        # Remove duplicates while preserving order
        unique_products = []
        seen = set()
        for product, confidence, match_type in sorted_products:
            if product.lower() not in seen:
                unique_products.append(product)
                seen.add(product.lower())
        
        # Limit results based on context
        if has_buying_intent:
            max_results = 5 if is_subject else 7  # More results if buying intent
        else:
            max_results = 3 if is_subject else 5
        
        final_products = unique_products[:max_results]
        
        print(f"    DEBUG: Final products sorted by confidence ({len(final_products)}):")
        for i, product in enumerate(final_products):
            match_info = next((conf, mtype) for p, conf, mtype in sorted_products if p == product)
            print(f"    DEBUG:   {i+1}. '{product}' (confidence: {match_info[0]}, type: {match_info[1]})")
        
        return final_products
    
    return []

def analyze_lead_products(email: str, product_catalog: List[Dict], start_date: datetime, end_date: datetime) -> Dict:
    """Analyze a lead's product interests based on Freshdesk tickets"""
    result = {
        'email': email,
        'total_tickets': 0,
        'product_mentions': [],
        'ticket_subjects': [],
        'conversation_snippets': [],
        'analysis_period': f"{start_date.strftime('%B %Y')} - {end_date.strftime('%B %Y')}"
    }
    
    print(f"  DEBUG: Starting product analysis for {email}")
    print(f"  DEBUG: Product catalog has {len(product_catalog)} products")
    
    # Get tickets for the specified period
    tickets = get_tickets_for_email_in_period(email, start_date, end_date)
    result['total_tickets'] = len(tickets)
    
    print(f"  DEBUG: Lead analyzer found {len(tickets)} tickets for {email}")
    
    if not tickets:
        print(f"  DEBUG: No tickets found for {email}, skipping product analysis")
        return result
    
    all_product_mentions = []
    
    for i, ticket in enumerate(tickets):
        ticket_id = ticket.get('id')
        subject = ticket.get('subject') or ''
        description = ticket.get('description_text') or ''
        
        print(f"    DEBUG: Analyzing ticket {i+1}/{len(tickets)} - ID: {ticket_id}")
        print(f"    DEBUG: Subject: {subject}")
        print(f"    DEBUG: Available ticket fields: {list(ticket.keys())}")
        
        # Check custom fields
        if 'custom_fields' in ticket:
            print(f"    DEBUG: Custom fields: {ticket['custom_fields']}")
        
        # Only add subject if not empty
        if subject:
            result['ticket_subjects'].append(subject)
        
        # Extract products from ticket subject (highest priority)
        print(f"    DEBUG: Extracting products from subject...")
        subject_products = extract_product_mentions(subject, product_catalog, is_subject=True)
        print(f"    DEBUG: Found {len(subject_products)} products in subject")
        all_product_mentions.extend(subject_products)
        
        # Extract products from description (lower priority, only if subject didn't yield much)
        if len(subject_products) < 2 and description:
            print(f"    DEBUG: Extracting products from description...")
            desc_products = extract_product_mentions(description, product_catalog, is_subject=False)
            print(f"    DEBUG: Found {len(desc_products)} products in description")
            all_product_mentions.extend(desc_products)
        
        # Get conversations for this ticket
        print(f"    DEBUG: Getting conversations for ticket {ticket_id}...")
        conversations = get_ticket_conversations(ticket_id)
        print(f"    DEBUG: Found {len(conversations)} conversations")
        
        # Only analyze first few conversations to avoid noise
        for j, conv in enumerate(conversations[:3]):  # Limit to first 3 conversations
            if isinstance(conv, dict):
                # Get body text with null safety
                body_text = conv.get('body_text') or conv.get('body') or ''
                user_id = conv.get('user_id')
                from_email_raw = conv.get('from_email', '')
                from_email = from_email_raw.lower() if from_email_raw else ''
                
                # Skip if body is None or empty
                if not body_text:
                    print(f"      DEBUG: Empty conversation {j+1}")
                    continue
                
                # Skip auto-generated notes
                if is_auto_generated_note(body_text):
                    print(f"      DEBUG: Skipping auto-generated note {j+1}")
                    continue
                
                # Focus on customer messages (not internal staff responses)
                is_customer_message = (from_email and email.lower() in from_email) or not user_id
                
                if is_customer_message:
                    print(f"      DEBUG: Analyzing customer conversation {j+1}: {body_text[:50]}...")
                    # Store snippet for analysis
                    snippet = body_text[:200] + "..." if len(body_text) > 200 else body_text
                    result['conversation_snippets'].append(snippet)
                    
                    # Extract products from customer conversation only
                    conv_products = extract_product_mentions(body_text, product_catalog, is_subject=False)
                    print(f"      DEBUG: Found {len(conv_products)} products in customer message")
                    all_product_mentions.extend(conv_products)
                else:
                    print(f"      DEBUG: Skipping staff message {j+1}")
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)
    
    # Group and simplify product mentions
    simplified_products = simplify_product_mentions(all_product_mentions)
    result['product_mentions'] = simplified_products
    
    print(f"  DEBUG: Final product analysis for {email}:")
    print(f"  DEBUG: Total product mentions found: {len(simplified_products)}")
    print(f"  DEBUG: Products: {simplified_products}")
    
    return result

def simplify_product_mentions(product_mentions: List[str]) -> List[str]:
    """Remove duplicates and return specific product names without generic grouping"""
    if not product_mentions:
        return []
    
    # Remove exact duplicates while preserving order
    seen = set()
    unique_mentions = []
    
    for product in product_mentions:
        product_lower = product.lower()
        if product_lower not in seen:
            unique_mentions.append(product)
            seen.add(product_lower)
    
    # Sort alphabetically for consistent output and limit to top 7 most relevant
    return sorted(unique_mentions)[:7]

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
    if not product_catalog:
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
    """Standalone main function for testing with real emails"""
    start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
    
    # Test with actual not_spam_leads.csv
    if os.path.exists("./output/not_spam_leads.csv"):
        print_colored("Testing with actual not_spam_leads.csv...", Colors.GREEN)
        # Read first 3 emails from the file
        test_emails = []
        try:
            with open("./output/not_spam_leads.csv", 'r') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i < 3:  # Test first 3 emails
                        test_emails.append(row['email'])
                    else:
                        break
        except Exception as e:
            print_colored(f"Error reading not_spam_leads.csv: {e}", Colors.RED)
            test_emails = ["james.lim@outlook.com", "david@aeacorp.com.sg", "admin@sgcarstore.com"]
        
        # Test these real emails
        for email in test_emails:
            test_ticket_comparison(email, start_date, end_date)
            print()
    else:
        print_colored("not_spam_leads.csv not found, using test emails...", Colors.YELLOW)
        test_emails = ["james.lim@outlook.com", "david@aeacorp.com.sg", "admin@sgcarstore.com"]
        
        for email in test_emails:
            test_ticket_comparison(email, start_date, end_date)
            print()
    
    # Run full analysis
    return analyze_leads()

if __name__ == "__main__":
    main()
