
#!/usr/bin/env python3
"""
Simple Google Search Console Connection Test

This script provides a quick way to test your GSC setup and connection.
Run this to verify your credentials and property configuration are working.
"""

import os
import json
from datetime import datetime, timedelta

print("Testing Google Search Console Setup...")
print("-" * 50)

# Check environment variables
has_creds = bool(os.environ.get('GSC_CREDENTIALS'))
has_url = bool(os.environ.get('GSC_PROPERTY_URL'))

print(f"GSC_CREDENTIALS in Secrets: {'✓ Found' if has_creds else '✗ Not found'}")
print(f"GSC_PROPERTY_URL in Secrets: {'✓ Found' if has_url else '✗ Not found'}")

if has_url:
    print(f"Property URL: {os.environ.get('GSC_PROPERTY_URL')}")

if not (has_creds and has_url):
    print("\n❌ Missing required configuration")
    print("\nTo fix this:")
    print("1. Go to Replit Secrets (Tools > Secrets)")
    print("2. Add GSC_CREDENTIALS with your service account JSON")
    print("3. Add GSC_PROPERTY_URL with your website URL")
    exit(1)

print("\n" + "-" * 50)
print("Testing GSC Connection...")

try:
    from modules.gsc_client import GoogleSearchConsoleClient
    
    client = GoogleSearchConsoleClient()
    success = client.authenticate()
    
    if success:
        print("✓ Authentication successful!")
        
        # Try to fetch some data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print(f"\nFetching data from {start_date.date()} to {end_date.date()}...")
        
        data = client.get_search_queries(start_date, end_date, limit=5)
        
        if data is not None:
            print(f"✓ Retrieved {len(data)} queries")
            
            if not data.empty:
                print("\nTop queries:")
                for _, row in data.head().iterrows():
                    print(f"  - '{row['query']}': {row['clicks']} clicks, pos {row['position']:.1f}")
                
                print(f"\n🎉 GSC integration is working!")
                print(f"Total clicks in last 7 days: {data['clicks'].sum()}")
                print(f"Total impressions: {data['impressions'].sum()}")
            else:
                print("\n⚠️  No recent data available (normal for new properties)")
                print("✓ Connection successful but no data in last 7 days")
        else:
            print("✗ Failed to retrieve data")
    else:
        print("✗ Authentication failed")
        print("\nCheck:")
        print("- Service account email added to GSC property")
        print("- Property URL format (https://example.com/)")
        print("- API permissions in Google Cloud Console")

except ImportError as e:
    print(f"\n❌ Import error: {e}")
    print("Google API libraries may not be installed")
    print("Try running: pip install google-auth google-auth-oauthlib google-api-python-client")
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
    print("\nCommon solutions:")
    print("- Verify GSC_CREDENTIALS is valid JSON")
    print("- Check GSC_PROPERTY_URL format")
    print("- Ensure service account has GSC access")
