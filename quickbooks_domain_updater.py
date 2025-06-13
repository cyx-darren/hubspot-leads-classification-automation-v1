
import os
import csv
import requests
import json
import base64
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
    print(f"✓ Found {len(domains)} unique domains")
    return domains

def save_domains_to_csv(domains: Set[str], filename: str = 'Unique_Email_Domains.csv'):
    """Save unique domains to CSV file"""
    try:
        # Sort domains for consistent output
        sorted_domains = sorted(domains)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(['domain'])
            
            # Write domains
            for domain in sorted_domains:
                writer.writerow([domain])
        
        print(f"✓ Saved {len(domains)} unique domains to {filename}")
        
        # Show sample domains
        if domains:
            print(f"\nSample domains (first 10):")
            for i, domain in enumerate(sorted(domains)[:10]):
                print(f"  {i+1}. {domain}")
            if len(domains) > 10:
                print(f"  ... and {len(domains) - 10} more")
        
    except Exception as e:
        print(f"✗ Error saving domains to CSV: {e}")

def main():
    print("=== QuickBooks Domain Updater ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Initialize QuickBooks API
        print("\n1. Initializing QuickBooks API...")
        qb_api = QuickBooksAPI()
        
        # Fetch customers
        print("\n2. Fetching customers...")
        customers = qb_api.get_customers()
        
        if not customers:
            print("No customers found in QuickBooks")
            return
        
        # Extract domains
        print("\n3. Extracting domains...")
        domains = extract_domains_from_customers(customers)
        
        if not domains:
            print("No email domains found")
            return
        
        # Save to CSV
        print("\n4. Saving domains to CSV...")
        save_domains_to_csv(domains)
        
        print(f"\n✓ Domain extraction completed successfully!")
        print(f"  - Processed {len(customers)} customers")
        print(f"  - Found {len(domains)} unique email domains")
        print(f"  - Saved to Unique_Email_Domains.csv")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
