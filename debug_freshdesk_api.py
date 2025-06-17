
#!/usr/bin/env python3
"""
Debug script to show exact Freshdesk API URLs and test different endpoints
"""

import os
import requests
from requests.auth import HTTPBasicAuth

def debug_freshdesk_api():
    """Debug the exact API calls being made"""
    
    FRESHDESK_API_KEY = os.environ.get('FRESHDESK_API_KEY')
    FRESHDESK_DOMAIN = os.environ.get('FRESHDESK_DOMAIN')
    
    if not FRESHDESK_API_KEY or not FRESHDESK_DOMAIN:
        print("❌ Missing Freshdesk credentials")
        return
    
    print(f"🔍 Testing API with domain: {FRESHDESK_DOMAIN}")
    print(f"🔑 API key length: {len(FRESHDESK_API_KEY)} characters")
    
    # Test email
    test_email = "steven.riddle@icloud.com"
    
    auth = HTTPBasicAuth(FRESHDESK_API_KEY, "X")
    headers = {"Content-Type": "application/json"}
    
    # Test 1: Current approach (failing)
    print(f"\n1️⃣ CURRENT APPROACH (failing with 400)")
    search_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/search/tickets"
    params = {"query": f"email:{test_email}"}
    
    print(f"URL: {search_url}")
    print(f"Params: {params}")
    print(f"Full URL: {search_url}?query=email:{test_email}")
    
    try:
        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        print(f"Status: {response.status_code}")
        if response.status_code != 200:
            print(f"Error response: {response.text[:200]}")
    except Exception as e:
        print(f"Exception: {e}")
    
    # Test 2: Different search syntax
    print(f"\n2️⃣ ALTERNATIVE SEARCH SYNTAX")
    params2 = {"query": f'email:"{test_email}"'}  # With quotes
    print(f"URL: {search_url}")
    print(f"Params: {params2}")
    
    try:
        response = requests.get(search_url, headers=headers, auth=auth, params=params2)
        print(f"Status: {response.status_code}")
        if response.status_code != 200:
            print(f"Error response: {response.text[:200]}")
    except Exception as e:
        print(f"Exception: {e}")
    
    # Test 3: Filter endpoint
    print(f"\n3️⃣ FILTER ENDPOINT")
    filter_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets"
    filter_params = {"email": test_email}
    
    print(f"URL: {filter_url}")
    print(f"Params: {filter_params}")
    
    try:
        response = requests.get(filter_url, headers=headers, auth=auth, params=filter_params)
        print(f"Status: {response.status_code}")
        if response.status_code != 200:
            print(f"Error response: {response.text[:200]}")
        else:
            data = response.json()
            print(f"Success! Found {len(data)} tickets")
    except Exception as e:
        print(f"Exception: {e}")
    
    # Test 4: Test basic API connectivity
    print(f"\n4️⃣ BASIC API CONNECTIVITY TEST")
    basic_url = f"https://{FRESHDESK_DOMAIN}.freshdesk.com/api/v2/tickets"
    
    try:
        response = requests.get(basic_url, headers=headers, auth=auth, params={"per_page": 1})
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Basic API connectivity works")
        else:
            print(f"❌ Basic API failed: {response.text[:200]}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    debug_freshdesk_api()
