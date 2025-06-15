
#!/usr/bin/env python3
"""
Google Search Console Setup Test Script

This script validates your GSC integration setup and tests the connection.
Run this after setting up GSC credentials to verify everything works.
"""

import os
import sys
from datetime import datetime, timedelta

def print_colored(text: str, color: str = ""):
    """Print text with color codes"""
    colors = {
        "red": '\033[91m',
        "green": '\033[92m',
        "yellow": '\033[93m',
        "blue": '\033[94m',
        "bold": '\033[1m',
        "end": '\033[0m'
    }
    if color and color in colors:
        print(f"{colors[color]}{text}{colors['end']}")
    else:
        print(text)

def test_gsc_connection():
    """Test GSC connection and authentication"""
    print_colored("=== Google Search Console Setup Test ===\n", "bold")
    
    try:
        from modules.gsc_client import GoogleSearchConsoleClient, get_gsc_credentials, get_property_url
        
        # Step 1: Check credentials
        print("1. Checking GSC Credentials...")
        print("-" * 30)
        
        credentials = get_gsc_credentials()
        if credentials:
            print_colored("✓ GSC credentials found", "green")
            if 'client_email' in credentials:
                print(f"  Service account: {credentials['client_email']}")
            else:
                print_colored("✗ Invalid credentials format - missing client_email", "red")
                return False
        else:
            print_colored("✗ GSC credentials not found", "red")
            print("  Add GSC_CREDENTIALS to Replit Secrets or upload credentials file")
            return False
        
        # Step 2: Check property URL
        print("\n2. Checking Property URL...")
        print("-" * 30)
        
        property_url = get_property_url()
        print(f"  Property URL: {property_url}")
        
        # Step 3: Test authentication
        print("\n3. Testing Authentication...")
        print("-" * 30)
        
        client = GoogleSearchConsoleClient()
        if client.authenticate("temp_credentials", property_url):
            print_colored("✓ GSC authentication successful!", "green")
            
            # Step 4: Test data retrieval
            print("\n4. Testing Data Retrieval...")
            print("-" * 30)
            
            try:
                # Try to fetch recent data (last 7 days)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                
                test_data = client.get_search_queries(start_date, end_date, limit=5)
                
                if test_data is not None and not test_data.empty:
                    print_colored(f"✓ Retrieved {len(test_data)} test queries", "green")
                    print("Sample queries:")
                    for idx, row in test_data.head(3).iterrows():
                        print(f"  - '{row['query']}': {row['clicks']} clicks, pos {row['position']:.1f}")
                    return True
                elif test_data is not None:
                    print_colored("⚠️  No recent data available (normal for new properties)", "yellow")
                    return True
                else:
                    print_colored("✗ Failed to retrieve data", "red")
                    return False
                    
            except Exception as e:
                print_colored(f"✗ Data retrieval error: {e}", "red")
                return False
                
        else:
            print_colored("✗ GSC authentication failed", "red")
            print("Check:")
            print("  - Service account email added to GSC property")
            print("  - Property URL format (https://example.com/)")
            print("  - API permissions")
            return False
            
    except ImportError as e:
        print_colored(f"✗ Import error: {e}", "red")
        print("Google API libraries may not be installed")
        return False
    except Exception as e:
        print_colored(f"✗ Test error: {e}", "red")
        return False

def show_setup_instructions():
    """Show setup instructions if test fails"""
    print_colored("\n=== Setup Instructions ===", "bold")
    print("If the test failed, follow these steps:")
    print("\n1. Enable Google Search Console API:")
    print("   - Go to Google Cloud Console")
    print("   - Enable Search Console API")
    print("   - Create service account")
    print("   - Download JSON credentials")
    
    print("\n2. Add service account to GSC:")
    print("   - Go to Google Search Console")
    print("   - Select your property")
    print("   - Add service account email as user")
    
    print("\n3. Configure in Replit:")
    print("   - Option A: Add GSC_CREDENTIALS to Secrets")
    print("   - Option B: Upload file to data/credentials/gsc_credentials.json")
    print("   - Set GSC_PROPERTY_URL in Secrets")
    
    print(f"\n4. Run this test again:")
    print(f"   python test_gsc_setup.py")

def main():
    """Main test function"""
    success = test_gsc_connection()
    
    if success:
        print_colored("\n🎉 GSC setup is working correctly!", "green")
        print_colored("You can now use GSC integration in traffic attribution", "green")
    else:
        print_colored("\n❌ GSC setup needs attention", "red")
        show_setup_instructions()
    
    return 0 if success else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_colored("\nTest interrupted by user", "yellow")
        sys.exit(1)
    except Exception as e:
        print_colored(f"\nUnexpected error: {e}", "red")
        sys.exit(1)
