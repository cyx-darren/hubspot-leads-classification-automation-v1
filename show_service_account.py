
#!/usr/bin/env python3
"""
Service Account Information Helper

This script displays the service account email from GSC_CREDENTIALS
for easy copying when setting up Google Search Console permissions.
"""

import os
import json

def main():
    print("Google Service Account Information")
    print("=" * 50)
    
    # Get credentials from environment
    creds_json = os.environ.get('GSC_CREDENTIALS')
    if not creds_json:
        print("❌ GSC_CREDENTIALS not found in environment")
        print("\nTo fix this:")
        print("1. Go to Replit Secrets (Tools > Secrets)")
        print("2. Add GSC_CREDENTIALS with your service account JSON")
        return
    
    try:
        creds = json.loads(creds_json)
        
        client_email = creds.get('client_email', 'Not found')
        project_id = creds.get('project_id', 'Not found')
        client_id = creds.get('client_id', 'Not found')
        
        print(f"\n📧 Service Account Email: {client_email}")
        print(f"🆔 Project ID: {project_id}")
        print(f"🔑 Client ID: {client_id}")
        
        print("\n📋 Copy the service account email above and follow these steps:")
        print("-" * 60)
        print("1. Go to Google Search Console")
        print("2. Select your property: https://easyprintsg.com/")
        print("3. Go to Settings → Users and permissions")
        print("4. Click 'Add user'")
        print("5. Paste the service account email:")
        print(f"   {client_email}")
        print("6. Choose 'Full' access (recommended)")
        print("7. Click 'Add'")
        
        print("\n✅ After adding the service account to GSC:")
        print("   Run: python test_gsc_simple.py")
        print("   This will test the connection and verify setup")
        
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in GSC_CREDENTIALS")
        print(f"   Details: {e}")
        print("\nThe GSC_CREDENTIALS should be valid JSON from Google Cloud")
    except Exception as e:
        print(f"❌ Error reading credentials: {e}")

if __name__ == "__main__":
    main()
