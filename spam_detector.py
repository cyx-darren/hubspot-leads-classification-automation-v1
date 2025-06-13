import csv
import requests
import json
import os
import sys
import time
from typing import List, Dict, Tuple
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone
import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API configuration - get from environment variables
FRESHDESK_API_KEY = os.environ.get('FRESHDESK_API_KEY')
FRESHDESK_DOMAIN = os.environ.get('FRESHDESK_DOMAIN')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')  # For potential future use

# Date range for analysis
START_DATE = datetime(2025, 3, 1, tzinfo=timezone.utc)  # March 1, 2025
END_DATE = datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc)  # May 31, 2025

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

def read_whitelist(file_path: str) -> List[str]:
    """Read the whitelist CSV file and return a list of whitelisted domains."""
    whitelisted_domains = []
    try:
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)

            # Check if first row is header
            try:
                first_row = next(csv_reader, None)
                if first_row:
                    # If it looks like a domain, add it
                    potential_domain = first_row[0].strip().lower() if first_row else ""
                    if potential_domain and ('.' in potential_domain) and ('@' not in potential_domain):
                        whitelisted_domains.append(potential_domain)
            except StopIteration:
                print_colored("Warning: Whitelist file appears to be empty", Colors.YELLOW)
                return whitelisted_domains

            # Read remaining domains
            for row in csv_reader:
                if row and row[0]:  # Ensure row and first value exist
                    domain = row[0].strip().lower()
                    if domain and ('.' in domain) and ('@' not in domain):
                        whitelisted_domains.append(domain)
                    elif domain:
                        print_colored(f"Warning: Skipping invalid domain format: {domain}", Colors.YELLOW)

        return whitelisted_domains
    except FileNotFoundError:
        print_colored(f"Error: Whitelist file not found: {file_path}", Colors.RED)
        return []
    except Exception as e:
        print_colored(f"Error reading whitelist file: {e}", Colors.RED)
        return []

def read_emails_from_file(file_path: str) -> List[str]:
    """Read the list of emails to check from a file (CSV or plain text)."""
    emails = []
    try:
        with open(file_path, 'r') as file:
            # Check if file is CSV based on extension
            if file_path.lower().endswith('.csv'):
                csv_reader = csv.reader(file)
                # Check if there's a header row
                potential_header = next(csv_reader, None)

                # Process first row if it exists
                if potential_header:
                    # Look for email column
                    email_col_idx = None
                    for idx, col in enumerate(potential_header):
                        if col.lower() in ['email', 'email address', 'e-mail', 'email_address']:
                            email_col_idx = idx
                            break

                    # If first row doesn't have email column headers, it might be data
                    if email_col_idx is None:
                        for idx, val in enumerate(potential_header):
                            if val and '@' in val:  # Looks like an email
                                emails.append(val.strip())
                                email_col_idx = idx
                                break

                # Default to first column if we couldn't identify email column
                if email_col_idx is None:
                    email_col_idx = 0
                    print_colored(f"Warning: Could not identify email column in CSV. Using first column.", Colors.YELLOW)

                # Process the rest of the rows
                for row in csv_reader:
                    if row and len(row) > email_col_idx:
                        email = row[email_col_idx].strip()
                        if email and '@' in email:
                            emails.append(email)
                        elif email:
                            print_colored(f"Warning: Skipping invalid email format: {email}", Colors.YELLOW)
            else:
                # Plain text file with one email per line
                for line in file:
                    email = line.strip()
                    if email and '@' in email:  # Basic validation
                        emails.append(email)
                    elif email:  # Non-empty but invalid
                        print_colored(f"Warning: Skipping invalid email format: {email}", Colors.YELLOW)
        return emails
    except FileNotFoundError:
        print_colored(f"Error: Email list file not found: {file_path}", Colors.RED)
        return []
    except Exception as e:
        print_colored(f"Error reading emails file: {e}", Colors.RED)
        return []

def extract_domain(email: str) -> str:
    """Extract the domain part from an email address."""
    try:
        return email.split('@')[1].lower()
    except IndexError:
        print_colored(f"Invalid email format: {email}", Colors.YELLOW)
        return ""

def is_whitelisted(email: str, whitelist: List[str]) -> bool:
    """Check if the email's domain is in the whitelist."""
    domain = extract_domain(email)
    return domain in whitelist

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

def is_ticket_in_date_range(ticket: Dict) -> bool:
    """Check if a ticket was created within the specified date range."""
    created_at = ticket.get('created_at')
    if not created_at:
        return False

    ticket_date = parse_ticket_date(created_at)
    if not ticket_date:
        return False

    # Ensure ticket_date is timezone-aware
    if ticket_date.tzinfo is None:
        ticket_date = ticket_date.replace(tzinfo=timezone.utc)

    return START_DATE <= ticket_date <= END_DATE

def get_tickets_for_email(email: str) -> List[Dict]:
    """
    Get all tickets associated with an email from Freshdesk within the date range.
    Tries multiple API approaches to handle different Freshdesk configurations.
    """
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print_colored("Error: Freshdesk API credentials not set", Colors.RED)
        return []

    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}
    all_tickets = []

    # Approach 1: Try search query with date filters
    try:
        search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
        # Format dates for Freshdesk query
        start_str = START_DATE.strftime("%Y-%m-%d")
        end_str = END_DATE.strftime("%Y-%m-%d")

        # Try with date range in query
        query = f"email:'{email}' AND created_at:>'{start_str}' AND created_at:<'{end_str}'"
        params = {"query": query}

        response = requests.get(search_url, headers=headers, auth=auth, params=params)

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets to ensure they're in date range
                filtered_tickets = [t for t in tickets if is_ticket_in_date_range(t)]
                all_tickets.extend(filtered_tickets)
                return all_tickets
    except Exception as e:
        print_colored(f"Search query with date filter approach failed: {e}", Colors.YELLOW)

    # Approach 2: Get all tickets for email and filter by date
    try:
        search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
        params = {"query": f"email:'{email}'"}

        response = requests.get(search_url, headers=headers, auth=auth, params=params)

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets by date range
                filtered_tickets = [t for t in tickets if is_ticket_in_date_range(t)]
                all_tickets.extend(filtered_tickets)
                return all_tickets
    except Exception as e:
        print_colored(f"Search query approach failed: {e}", Colors.YELLOW)

    # Approach 3: Try direct filter query
    try:
        filter_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets"
        params = {"email": email}

        response = requests.get(filter_url, headers=headers, auth=auth, params=params)

        if response.status_code == 200:
            tickets = response.json()
            if tickets:
                # Filter tickets by date range
                filtered_tickets = [t for t in tickets if is_ticket_in_date_range(t)]
                all_tickets.extend(filtered_tickets)
                return all_tickets
    except Exception as e:
        print_colored(f"Filter query approach failed: {e}", Colors.YELLOW)

    # If we've reached here, we couldn't get tickets using any approach
    if not all_tickets:
        print_colored(f"Could not retrieve tickets for {email} using available API methods", Colors.YELLOW)

    return all_tickets

def check_sales_response_in_ticket(ticket_id: int) -> Tuple[bool, str]:
    """
    Check if the ticket contains responses from sales team with quotation text.
    Returns a tuple of (is_sales_interaction, details)
    """
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print_colored("Error: Freshdesk API credentials not set", Colors.RED)
        return False, "API credentials not set"

    url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets/{ticket_id}/conversations"
    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}

    # List of specific phrases that indicate sales team interaction (multi-word phrases only)
    sales_phrases = [
        "have attached the quotation for your kind consideration",
        "attached the quotation for your kind consideration",
        "quotation for your kind consideration",
        "attached the quotation",
        "quotation is inclusive of free delivery",
        "attached the digital mock-up",
        "mock-up for your visualization",
        "perhaps you'd like to share your logo/design",
        "create the digital mock-up for your visualization",
        "have attached the digital mock-up for your visualization",
        "thank you for your enquiry",
        "thank you for your inquiry"
    ]

    try:
        response = requests.get(url, headers=headers, auth=auth)

        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            print_colored(f"Rate limited by Freshdesk API. Waiting {retry_after} seconds...", Colors.YELLOW)
            time.sleep(retry_after)
            return check_sales_response_in_ticket(ticket_id)  # Retry after waiting

        response.raise_for_status()
        conversations = response.json()

        # Look for sales team responses with quotation text
        for conv in conversations:
            if not isinstance(conv, dict):
                continue

            # Check in body_text (plain text) field
            body_text = conv.get("body_text", "").lower() if conv.get("body_text") else ""
            body_html = conv.get("body", "").lower() if conv.get("body") else ""

            for phrase in sales_phrases:
                if phrase in body_text or phrase in body_html:
                    return True, f"Found sales phrase: '{phrase}'"

            # Check for sales team signatures combined with context
            signatures = ["sales executive", "team lead", "corporate accounts", 
                         "warmest regards", "easyprint technologies"]

            # Check if signature exists along with sales-related context
            for signature in signatures:
                if signature in body_text or signature in body_html:
                    # Only count signatures if they appear with sales context
                    context_phrases = ["thank you", "regards", "quotation", "attached"]
                    for context in context_phrases:
                        if context in body_text or context in body_html:
                            return True, f"Found sales signature with context: '{signature}' with '{context}'"

        return False, "No sales interactions found in conversations"

    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            error_msg += f" | Response: {e.response.text}"
        return False, f"Error retrieving conversations: {error_msg}"

def classify_email(email: str, whitelist: List[str]) -> Dict:
    """
    Classify an email as spam or not spam based on the given criteria.
    Returns a dictionary with classification details.
    """
    result = {
        "email": email,
        "classification": "",
        "reason": "",
        "details": {}
    }

    # Step 1: Check whitelist
    domain = extract_domain(email)
    if domain in whitelist:
        result["classification"] = "Not Spam"
        result["reason"] = "Whitelisted domain"
        result["details"]["domain"] = domain
        return result

    # Step 2 & 3: Check Freshdesk ticket history for March-May 2025
    tickets = get_tickets_for_email(email)

    if not tickets:
        result["classification"] = "Spam"
        result["reason"] = f"No ticket history in period {START_DATE.strftime('%B %Y')} - {END_DATE.strftime('%B %Y')}"
        return result

    # Step 3: Check for sales team responses in tickets
    result["details"]["ticket_count"] = len(tickets)
    result["details"]["ticket_ids"] = []
    result["details"]["sales_checks"] = []
    result["details"]["date_range"] = f"{START_DATE.strftime('%B %d, %Y')} - {END_DATE.strftime('%B %d, %Y')}"

    for ticket in tickets:
        if "id" in ticket:
            ticket_id = ticket["id"]
            created_at = ticket.get("created_at", "")
            result["details"]["ticket_ids"].append(f"{ticket_id} (created: {created_at})")

            # Get ticket details
            has_sales_interaction, interaction_details = check_sales_response_in_ticket(ticket_id)

            result["details"]["sales_checks"].append({
                "ticket_id": ticket_id,
                "created_at": created_at,
                "has_sales_interaction": has_sales_interaction,
                "details": interaction_details
            })

            if has_sales_interaction:
                result["classification"] = "Not Spam"
                result["reason"] = f"Sales team interaction found in ticket {ticket_id}: {interaction_details}"
                return result

    # Step 4: Default case - all other emails with tickets but no sales responses
    result["classification"] = "Spam"
    result["reason"] = f"No sales team interaction found in {len(tickets)} tickets during {START_DATE.strftime('%B-%B %Y')}"

    # Add detailed explanation about why no sales interaction was found
    if "sales_checks" in result["details"] and result["details"]["sales_checks"]:
        reasons = [check["details"] for check in result["details"]["sales_checks"]]
        result["reason"] += f" - Details: {'; '.join(reasons)}"

    return result

def save_results_to_csv(results: List[Dict]):
    """Save classification results to separate CSV files for spam and not spam."""
    # Separate results by classification
    not_spam_results = [r for r in results if r['classification'] == 'Not Spam']
    spam_results = [r for r in results if r['classification'] == 'Spam']
    
    fieldnames = ['email', 'classification', 'reason', 'ticket_count']
    
    # Save not spam emails
    try:
        with open('not_spam_leads.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in not_spam_results:
                ticket_count = 0
                if "details" in result and "ticket_count" in result["details"]:
                    ticket_count = result["details"]["ticket_count"]
                
                writer.writerow({
                    'email': result['email'],
                    'classification': result['classification'],
                    'reason': result['reason'],
                    'ticket_count': ticket_count
                })
        
        print_colored(f"Not spam results saved to not_spam_leads.csv ({len(not_spam_results)} emails)", Colors.GREEN)
    except Exception as e:
        print_colored(f"Error saving not spam results to CSV: {e}", Colors.RED)
    
    # Save spam emails
    try:
        with open('spam_leads.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in spam_results:
                ticket_count = 0
                if "details" in result and "ticket_count" in result["details"]:
                    ticket_count = result["details"]["ticket_count"]
                
                writer.writerow({
                    'email': result['email'],
                    'classification': result['classification'],
                    'reason': result['reason'],
                    'ticket_count': ticket_count
                })
        
        print_colored(f"Spam results saved to spam_leads.csv ({len(spam_results)} emails)", Colors.GREEN)
    except Exception as e:
        print_colored(f"Error saving spam results to CSV: {e}", Colors.RED)
    
    return len(not_spam_results), len(spam_results)

def main():
    print_colored("\n=== Spam Detector (March - May 2025) ===", Colors.BOLD + Colors.BLUE)
    print_colored(f"Analyzing tickets from {START_DATE.strftime('%B %d, %Y')} to {END_DATE.strftime('%B %d, %Y')}", Colors.BLUE)

    # Check if API keys are set
    if not FRESHDESK_API_KEY:
        print_colored("Error: FRESHDESK_API_KEY environment variable not set", Colors.RED)
        sys.exit(1)
    if not FRESHDESK_DOMAIN:
        print_colored("Error: FRESHDESK_DOMAIN environment variable not set", Colors.RED)
        sys.exit(1)

    # Path to whitelist file
    whitelist_file = "Unique_Email_Domains.csv"

    # Read whitelist
    print(f"\nReading whitelist from {whitelist_file}...")
    whitelisted_domains = read_whitelist(whitelist_file)
    if whitelisted_domains:
        print_colored(f"Successfully loaded {len(whitelisted_domains)} whitelisted domains", Colors.GREEN)
    else:
        print_colored("Warning: No whitelisted domains found", Colors.YELLOW)

    # Get list of emails to check - from leads.csv by default
    print("\nHow would you like to provide emails to check?")
    print("1. Read from leads.csv (default)")
    print("2. Read from another file")
    print("3. Enter manually")
    emails_input_choice = input("Enter your choice (1, 2, or 3) [1]: ").strip() or '1'

    emails_to_check = []
    if emails_input_choice == '1':
        email_file = "leads.csv"
        print(f"Reading emails from {email_file}...")
        emails_to_check = read_emails_from_file(email_file)
        if emails_to_check:
            print_colored(f"Successfully loaded {len(emails_to_check)} emails to check", Colors.GREEN)
        else:
            print_colored("No valid emails found in leads.csv. Exiting.", Colors.RED)
            sys.exit(1)
    elif emails_input_choice == '2':
        email_file = input("Enter the path to the email list file: ").strip()
        emails_to_check = read_emails_from_file(email_file)
        if emails_to_check:
            print_colored(f"Successfully loaded {len(emails_to_check)} emails to check", Colors.GREEN)
        else:
            print_colored("No valid emails found in the file. Exiting.", Colors.RED)
            sys.exit(1)
    elif emails_input_choice == '3':
        print("Enter emails one per line. Enter a blank line when finished:")
        while True:
            email = input().strip()
            if not email:
                break
            if '@' in email:
                emails_to_check.append(email)
            else:
                print_colored(f"Skipping invalid email: {email}", Colors.YELLOW)
        if emails_to_check:
            print_colored(f"Successfully entered {len(emails_to_check)} emails to check", Colors.GREEN)
        else:
            print_colored("No valid emails entered. Exiting.", Colors.RED)
            sys.exit(1)
    else:
        print_colored("Invalid choice. Exiting.", Colors.RED)
        sys.exit(1)

    # Process each email
    print_colored(f"\nProcessing emails for tickets from {START_DATE.strftime('%B %Y')} to {END_DATE.strftime('%B %Y')}...", Colors.BLUE)
    results = []

    for i, email in enumerate(emails_to_check):
        progress = f"[{i+1}/{len(emails_to_check)}]"
        print(f"\n{progress} Processing: {email}")

        classification_result = classify_email(email, whitelisted_domains)
        results.append(classification_result)

        if classification_result['classification'] == 'Spam':
            status_color = Colors.RED
        else:
            status_color = Colors.GREEN

        print_colored(f"{progress} Result: {classification_result['classification']} - {classification_result['reason']}", status_color)

        # Print more details for debugging if spam with ticket history
        if classification_result['classification'] == 'Spam' and 'ticket_count' in classification_result['details'] and classification_result['details']['ticket_count'] > 0:
            print("  Detailed check results:")
            if 'sales_checks' in classification_result['details']:
                for check in classification_result['details']['sales_checks']:
                    print(f"  - Ticket {check['ticket_id']} (created: {check['created_at']}): {check['details']}")

    # Save results
    not_spam_count, spam_count = save_results_to_csv(results)

    print_colored("\n=== Spam Detection Summary ===", Colors.BOLD + Colors.BLUE)
    print_colored(f"Analysis Period: {START_DATE.strftime('%B %d, %Y')} - {END_DATE.strftime('%B %d, %Y')}", Colors.BLUE)
    print(f"Total emails processed: {len(results)}")
    print(f"Spam emails detected: {spam_count}")
    print(f"Non-spam emails detected: {not_spam_count}")
    
    return not_spam_count, spam_count

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_colored("\nProcess interrupted by user. Exiting.", Colors.YELLOW)
        sys.exit(0)
    except Exception as e:
        print_colored(f"\nUnexpected error: {e}", Colors.RED)
        sys.exit(1)