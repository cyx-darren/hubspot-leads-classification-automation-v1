
import csv
import requests
import os
import shutil
from datetime import datetime
from typing import Set, List, Dict
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# QuickBooks API configuration
QB_CLIENT_ID = os.environ.get('QUICKBOOKS_CLIENT_ID')
QB_CLIENT_SECRET = os.environ.get('QUICKBOOKS_CLIENT_SECRET')
QB_COMPANY_ID = os.environ.get('QUICKBOOKS_COMPANY_ID')
QB_REFRESH_TOKEN = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')

# Generic domains to exclude from whitelist
GENERIC_DOMAINS = {
    'gmail.com',
    'qq.com',
    'hotmail.com',
    'hotmail.co.uk',
    'hotmail.sg',
    'yahoo.com',
    'yahoo.co.uk',
    'yahoo.com.sg',
    'outlook.com',
    'live.com',
    'icloud.com',
    'mail.com',
    'protonmail.com',
    'aol.com',
    'ymail.com',
    'msn.com',
    'me.com',
    'proton.me',
    'gmx.com',
    '163.com',
    '126.com',
    'cyberlinks7.onmicrosoft.com',
    'easyprintsg.com',
    'singnet.com.sg'
}

def print_colored(text: str, color: str):
    """Print text with color for better readability"""
    colors = {
        'RED': '\033[91m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'BLUE': '\033[94m',
        'ENDC': '\033[0m'
    }
    print(f"{colors.get(color, '')}{text}{colors.get('ENDC', '')}")

def get_access_token():
    """Get access token using refresh token"""
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    
    try:
        # Add debug info (safely showing only first 20 chars)
        print(f"Using refresh token: {QB_REFRESH_TOKEN[:20] if QB_REFRESH_TOKEN else 'None'}...")
        print(f"Client ID: {QB_CLIENT_ID[:20] if QB_CLIENT_ID else 'None'}...")
        
        response = requests.post(
            token_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': QB_REFRESH_TOKEN,
                'client_id': QB_CLIENT_ID,
                'client_secret': QB_CLIENT_SECRET
            }
        )
        
        print(f"Token response status: {response.status_code}")
        if response.status_code != 200:
            print(f"Token error response: {response.text}")
        
        # Check for specific error conditions
        if response.status_code == 401:
            error_text = response.text.lower()
            if "invalid_grant" in error_text or "unauthorized" in error_text:
                print_colored("\n⚠️  Refresh token appears to be expired or invalid!", 'YELLOW')
                print("To fix this:")
                print("1. Go to https://developer.intuit.com/app/developer/playground")
                print("2. Sign in and reauthorize your app")
                print("3. Copy the new Refresh Token")
                print("4. Update QUICKBOOKS_REFRESH_TOKEN in Replit Secrets")
                print("5. Also update QUICKBOOKS_COMPANY_ID if it changed")
                return None
        
        response.raise_for_status()
        token_data = response.json()
        return token_data.get('access_token')
        
    except Exception as e:
        print_colored(f"Token refresh error details: {str(e)}", 'RED')
        return None

def extract_main_domain(domain):
    """Extract main domain from subdomain
    e.g., consultant.udtrucks.com -> udtrucks.com
    """
    parts = domain.split('.')
    
    # Handle standard domains (e.g., subdomain.domain.com)
    if len(parts) >= 3 and parts[-2] not in ['com', 'co', 'org', 'net']:
        # Return last two parts for .com, .org, etc
        return '.'.join(parts[-2:])
    
    # Handle country-code domains (e.g., subdomain.domain.com.sg)
    elif len(parts) >= 4 and parts[-2] in ['com', 'co', 'org', 'net'] and len(parts[-1]) == 2:
        # Return last three parts for .com.sg, .co.uk, etc
        return '.'.join(parts[-3:])
    
    # Return as-is if it's already a main domain
    return domain

def extract_customer_domains(customers):
    """Extract unique domains from customer email addresses"""
    domains = set()
    generic_count = 0
    subdomain_count = 0
    
    for customer in customers:
        email = customer.get('PrimaryEmailAddr', {}).get('Address', '')
        if email and '@' in email:
            domain = email.split('@')[1].lower()
            
            # Skip generic domains
            if domain in GENERIC_DOMAINS:
                print(f"  Skipping generic domain: {domain}")
                generic_count += 1
                continue
            
            # Add the exact domain
            domains.add(domain)
            
            # Also add main domain if this is a subdomain
            main_domain = extract_main_domain(domain)
            if main_domain != domain:
                domains.add(main_domain)
                print(f"  Added main domain {main_domain} for subdomain {domain}")
                subdomain_count += 1
    
    print_colored(f"Excluded {generic_count} emails from generic domains", 'YELLOW')
    if subdomain_count > 0:
        print_colored(f"Added {subdomain_count} main domains from subdomains", 'BLUE')
    
    return domains

def get_quickbooks_customers():
    """Fetch customers from QuickBooks API"""
    access_token = get_access_token()
    if not access_token:
        return []
    
    base_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{QB_COMPANY_ID}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    try:
        url = f"{base_url}/query?query=SELECT * FROM Customer"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        customers = data.get('QueryResponse', {}).get('Customer', [])
        print_colored(f"Retrieved {len(customers)} customers from QuickBooks", 'GREEN')
        return customers
        
    except Exception as e:
        print_colored(f"Error fetching customers: {e}", 'RED')
        return []

def read_existing_domains_from_csv(filename: str) -> Set[str]:
    """Read existing domains from CSV file"""
    domains = set()
    try:
        with open(filename, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                if row and row[0].strip():
                    domains.add(row[0].strip().lower())
        print_colored(f"Read {len(domains)} existing domains from {filename}", 'BLUE')
    except FileNotFoundError:
        print_colored(f"No existing file found at {filename}", 'YELLOW')
    except Exception as e:
        print_colored(f"Error reading existing domains: {e}", 'RED')
    
    return domains

def backup_domain_file(filename: str, backup_filename: str):
    """Backup existing domain file"""
    try:
        # Create backup directory if it doesn't exist
        os.makedirs(os.path.dirname(backup_filename), exist_ok=True)
        shutil.copyfile(filename, backup_filename)
        print_colored(f"Backed up {filename} to {backup_filename}", 'GREEN')
    except FileNotFoundError:
        print_colored(f"No existing file to backup at {filename}", 'YELLOW')
    except Exception as e:
        print_colored(f"Error creating backup: {e}", 'RED')

def save_domains_to_csv(domains: Set[str], filename: str):
    """Save domains to CSV file"""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for domain in sorted(domains):
                writer.writerow([domain])
        
        print_colored(f"Saved {len(domains)} domains to {filename}", 'GREEN')
    except Exception as e:
        print_colored(f"Error saving domains to CSV: {e}", 'RED')

def main():
    """Main function to update domains from QuickBooks"""
    print_colored("Starting QuickBooks domain update...", 'BLUE')
    
    # Check if API credentials are set with detailed status
    print("Checking QuickBooks credentials...")
    if not all([QB_CLIENT_ID, QB_CLIENT_SECRET, QB_COMPANY_ID, QB_REFRESH_TOKEN]):
        print_colored("❌ Missing QuickBooks credentials in Replit Secrets", 'RED')
        print(f"  CLIENT_ID: {'✓' if QB_CLIENT_ID else '✗'}")
        print(f"  CLIENT_SECRET: {'✓' if QB_CLIENT_SECRET else '✗'}")
        print(f"  REFRESH_TOKEN: {'✓' if QB_REFRESH_TOKEN else '✗'}")
        print(f"  COMPANY_ID: {'✓' if QB_COMPANY_ID else '✗'}")
        print("\nRequired environment variables:")
        print("  - QUICKBOOKS_CLIENT_ID")
        print("  - QUICKBOOKS_CLIENT_SECRET") 
        print("  - QUICKBOOKS_COMPANY_ID")
        print("  - QUICKBOOKS_REFRESH_TOKEN")
        return 1
    else:
        print_colored("✓ All QuickBooks credentials found in Replit Secrets", 'GREEN')
    
    filename = './data/Unique_Email_Domains.csv'
    backup_filename = './backups/Unique_Email_Domains_backup.csv'
    
    # Backup existing file
    backup_domain_file(filename, backup_filename)
    
    # Read existing domains
    existing_domains = read_existing_domains_from_csv(filename)
    
    # Get customers from QuickBooks
    customers = get_quickbooks_customers()
    if not customers:
        print_colored("No customers retrieved from QuickBooks", 'YELLOW')
        return 1
    
    # Extract new domains
    new_domains = extract_customer_domains(customers)
    
    # Merge domains
    all_domains = existing_domains | new_domains
    new_count = len(new_domains - existing_domains)
    
    # Save merged domains
    save_domains_to_csv(all_domains, filename)
    
    print_colored(f"\nDomain update complete!", 'GREEN')
    print(f"Total domains: {len(all_domains)}")
    print(f"New domains added: {new_count}")
    
    if new_count > 0:
        print("New domains:")
        for domain in sorted(new_domains - existing_domains):
            print(f"  + {domain}")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
