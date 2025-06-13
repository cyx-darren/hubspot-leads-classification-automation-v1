
import os
import csv
import requests
import json
import base64
import shutil
from typing import List, Set
from datetime import datetime

# QuickBooks API configuration
QB_BASE_URL = "https://sandbox-quickbooks.api.intuit.com"  # Change to production: https://quickbooks.api.intuit.com
QB_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

class QuickBooksAPI:
    def __init__(self):
        self.client_id = os.environ.get('QUICKBOOKS_CLIENT_ID')
        self.client_secret = os.environ.get('QUICKBOOKS_CLIENT_SECRET')
        self.company_id = os.environ.get('QUICKBOOKS_COMPANY_ID')
        self.refresh_token = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')
        self.access_token = None
        
        # Validate required credentials
        if not all([self.client_id, self.client_secret, self.company_id, self.refresh_token]):
            raise ValueError("Missing QuickBooks API credentials in Replit Secrets")
    
    def get_access_token(self):
        """Get a new access token using the refresh token"""
        # Create basic auth header
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_credentials}'
        }
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }
        
        print("Requesting new access token...")
        response = requests.post(QB_TOKEN_URL, headers=headers, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)
            print(f"✓ Access token obtained (expires in {expires_in} seconds)")
            return True
        else:
            print(f"✗ Failed to get access token: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    
    def make_authenticated_request(self, url, params=None):
        """Make an authenticated request, refreshing token if needed"""
        if not self.access_token:
            if not self.get_access_token():
                raise Exception("Failed to obtain access token")
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        # If token expired, refresh and retry once
        if response.status_code == 401:
            print("Access token expired, refreshing...")
            if self.get_access_token():
                headers['Authorization'] = f'Bearer {self.access_token}'
                response = requests.get(url, headers=headers, params=params)
            else:
                raise Exception("Failed to refresh access token")
        
        return response
    
    def get_customers(self):
        """Fetch all customers from QuickBooks"""
        url = f"{QB_BASE_URL}/v3/company/{self.company_id}/query"
        
        all_customers = []
        start_position = 1
        max_results = 20  # QuickBooks API limit
        
        print("Fetching customers from QuickBooks...")
        
        while True:
            # Build query with pagination
            query = f"SELECT * FROM Customer STARTPOSITION {start_position} MAXRESULTS {max_results}"
            params = {'query': query}
            
            response = self.make_authenticated_request(url, params)
            
            if response.status_code == 200:
                data = response.json()
                query_response = data.get('QueryResponse', {})
                customers = query_response.get('Customer', [])
                
                if not customers:
                    break  # No more customers
                
                all_customers.extend(customers)
                print(f"  Fetched {len(customers)} customers (total: {len(all_customers)})")
                
                # Check if we got fewer results than requested (end of data)
                if len(customers) < max_results:
                    break
                
                start_position += max_results
            else:
                print(f"✗ Failed to fetch customers: {response.status_code}")
                print(f"Response: {response.text}")
                break
        
        print(f"✓ Total customers fetched: {len(all_customers)}")
        return all_customers

def read_existing_domains(filename: str) -> Set[str]:
    """Read existing domains from CSV file"""
    existing_domains = set()
    
    if not os.path.exists(filename):
        print(f"No existing file found: {filename}")
        return existing_domains
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            # Skip header if it exists
            first_row = next(reader, None)
            if first_row and first_row[0].lower() in ['domain', 'domains']:
                pass  # Skip header
            elif first_row:
                # First row is data
                domain = first_row[0].strip().lower()
                if domain and '.' in domain:
                    existing_domains.add(domain)
            
            # Read remaining domains
            for row in reader:
                if row and row[0]:
                    domain = row[0].strip().lower()
                    if domain and '.' in domain:
                        existing_domains.add(domain)
        
        print(f"✓ Read {len(existing_domains)} existing domains from {filename}")
        
    except Exception as e:
        print(f"✗ Error reading existing domains: {e}")
    
    return existing_domains

def create_backup(filename: str, backup_filename: str):
    """Create backup of existing file"""
    try:
        if os.path.exists(filename):
            shutil.copy2(filename, backup_filename)
            print(f"✓ Created backup: {backup_filename}")
        else:
            print(f"No existing file to backup: {filename}")
    except Exception as e:
        print(f"✗ Error creating backup: {e}")

def extract_domains_from_customers(customers: List[dict]) -> Set[str]:
    """Extract unique domains from customer email addresses"""
    domains = set()
    email_count = 0
    
    print("Extracting domains from customer emails...")
    
    for customer in customers:
        # Check PrimaryEmailAddr field
        primary_email = customer.get('PrimaryEmailAddr', {})
        if isinstance(primary_email, dict) and primary_email.get('Address'):
            email = primary_email['Address'].strip()
            if email and '@' in email:
                domain = email.split('@')[1].lower().strip()
                if domain and '.' in domain:
                    domains.add(domain)
                    email_count += 1
        
        # Check for other email fields that might exist
        if 'Email' in customer and customer['Email']:
            email = customer['Email'].strip()
            if email and '@' in email:
                domain = email.split('@')[1].lower().strip()
                if domain and '.' in domain:
                    domains.add(domain)
                    email_count += 1
    
    print(f"✓ Processed {email_count} email addresses")
    print(f"✓ Found {len(domains)} unique domains from QuickBooks")
    return domains

def save_merged_domains_to_csv(all_domains: Set[str], filename: str = 'Unique_Email_Domains.csv'):
    """Save merged domains to CSV file"""
    try:
        # Sort domains for consistent output
        sorted_domains = sorted(all_domains)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(['domain'])
            
            # Write domains
            for domain in sorted_domains:
                writer.writerow([domain])
        
        print(f"✓ Saved {len(all_domains)} total domains to {filename}")
        
    except Exception as e:
        print(f"✗ Error saving domains to CSV: {e}")

def main():
    print("=== QuickBooks Domain Updater (Merge Mode) ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    filename = 'Unique_Email_Domains.csv'
    backup_filename = 'Unique_Email_Domains_backup.csv'
    
    try:
        # Step 1: Read existing domains
        print("\n1. Reading existing domains...")
        existing_domains = read_existing_domains(filename)
        
        # Step 2: Create backup
        print("\n2. Creating backup...")
        create_backup(filename, backup_filename)
        
        # Step 3: Initialize QuickBooks API
        print("\n3. Initializing QuickBooks API...")
        qb_api = QuickBooksAPI()
        
        # Step 4: Fetch customers
        print("\n4. Fetching customers...")
        customers = qb_api.get_customers()
        
        if not customers:
            print("No customers found in QuickBooks")
            return 1
        
        # Step 5: Extract domains from QuickBooks
        print("\n5. Extracting domains...")
        qb_domains = extract_domains_from_customers(customers)
        
        if not qb_domains:
            print("No email domains found in QuickBooks")
            return 1
        
        # Step 6: Find new domains
        print("\n6. Comparing domains...")
        new_domains = qb_domains - existing_domains
        
        if new_domains:
            # Sort new domains for display
            sorted_new_domains = sorted(new_domains)
            
            print(f"✓ Found {len(new_domains)} new domains:")
            
            # Display new domains (limit to first 10 for readability)
            if len(new_domains) <= 10:
                print(f"  New domains: {', '.join(sorted_new_domains)}")
            else:
                print(f"  New domains: {', '.join(sorted_new_domains[:10])}... and {len(new_domains) - 10} more")
            
            # Step 7: Merge and save
            print("\n7. Merging and saving domains...")
            all_domains = existing_domains | qb_domains
            save_merged_domains_to_csv(all_domains, filename)
            
            print(f"\n✓ Domain update completed successfully!")
            print(f"  - Added {len(new_domains)} new domains")
            print(f"  - Total domains now: {len(all_domains)}")
            print(f"  - Backup saved as: {backup_filename}")
            
        else:
            print("✓ No new domains found - file is already up to date")
            print(f"  - QuickBooks domains: {len(qb_domains)}")
            print(f"  - Existing domains: {len(existing_domains)}")
            print(f"  - All QuickBooks domains already exist in the file")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
