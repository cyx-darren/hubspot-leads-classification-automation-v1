
#!/usr/bin/env python3
"""
Simple GA4 Connection Test
Tests Google Analytics 4 integration
"""

import os
import sys
from datetime import datetime, timedelta

def print_colored(text, color):
    """Print colored text for better visibility"""
    colors = {
        'green': '\033[92m',
        'yellow': '\033[93m', 
        'red': '\033[91m',
        'blue': '\033[94m',
        'bold': '\033[1m',
        'end': '\033[0m'
    }
    print(f"{colors.get(color, '')}{text}{colors['end']}")

def test_ga4_setup():
    """Test GA4 setup and configuration"""
    print("Testing Google Analytics 4 Setup...")
    print("=" * 50)
    
    # Check credentials
    ga4_creds = os.environ.get('GA4_CREDENTIALS')
    gsc_creds = os.environ.get('GSC_CREDENTIALS')  # Fallback
    property_id = os.environ.get('GA4_PROPERTY_ID')
    
    has_credentials = bool(ga4_creds or gsc_creds)
    
    if ga4_creds:
        print_colored("✓ GA4_CREDENTIALS found in Secrets", "green")
    elif gsc_creds:
        print_colored("✓ Using GSC_CREDENTIALS for GA4", "yellow")
    else:
        print_colored("✗ No GA4 credentials found", "red")
        print("Add GA4_CREDENTIALS to Replit Secrets with your service account JSON")
        return False
    
    if not property_id:
        print_colored("✗ GA4_PROPERTY_ID not set", "red")
        print("Add your GA4 property ID to Replit Secrets as GA4_PROPERTY_ID")
        return False
    print_colored(f"✓ GA4 property ID: {property_id}", "green")
    
    return True

def test_ga4_connection():
    """Test GA4 connection and show sample data"""
    print("\nTesting GA4 Connection...")
    print("=" * 50)
    
    try:
        from modules.ga4_client import GoogleAnalytics4Client
        
        client = GoogleAnalytics4Client()
        print("✓ GA4 client initialized")
        
        # Test authentication
        auth_success = client.authenticate()
        if not auth_success:
            print_colored("✗ GA4 authentication failed", "red")
            return False
        print_colored("✓ GA4 authentication successful", "green")
        
        # Get last 7 days of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print(f"\nFetching traffic data ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})...")
        
        traffic = client.get_traffic_by_source(start_date, end_date)
        
        if traffic is None or traffic.empty:
            print_colored("⚠️  No traffic data available for the selected period", "yellow")
            print("This could be normal for:")
            print("- New GA4 properties")
            print("- Properties with very low traffic")
            print("- Recent date ranges (GA4 has data processing delays)")
            return True
        
        print_colored(f"✓ GA4 connection successful!", "green")
        print_colored(f"✓ Retrieved {len(traffic)} traffic records", "green")
        
        if len(traffic) > 0:
            print("\nTop traffic sources (last 7 days):")
            summary = traffic.groupby(['source', 'medium'])['sessions'].sum().sort_values(ascending=False).head()
            for (source, medium), sessions in summary.items():
                print(f"  - {source} / {medium}: {sessions} sessions")
            
            total_sessions = traffic['sessions'].sum()
            print(f"\nTotal sessions: {total_sessions}")
        
        # Test hourly patterns
        print("\nTesting hourly traffic patterns...")
        try:
            hourly = client.get_hourly_traffic_patterns(
                datetime.now() - timedelta(days=1),
                datetime.now()
            )
            
            print_colored(f"✓ Retrieved {len(hourly)} hourly records", "green")
            
            if not hourly.empty:
                print("Recent hourly activity:")
                recent_hours = hourly.groupby('datetime')['sessions'].sum().tail(5)
                for dt, sessions in recent_hours.items():
                    print(f"  - {dt.strftime('%Y-%m-%d %H:00')}: {sessions} sessions")
        
        except Exception as e:
            print_colored(f"⚠️  Hourly patterns test failed: {e}", "yellow")
            print("This is not critical for basic GA4 functionality")
        
        print_colored("\n🎉 GA4 integration is working!", "green")
        return True
        
    except ImportError as e:
        print_colored("✗ GA4 client module not available", "red")
        print(f"Import error: {e}")
        print("Make sure google-analytics-data is installed")
        return False
    except Exception as e:
        print_colored(f"✗ GA4 connection test failed: {e}", "red")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print_colored("Google Analytics 4 Integration Test", "bold")
    print("=" * 60)
    
    # Test setup
    setup_ok = test_ga4_setup()
    if not setup_ok:
        print_colored("\n❌ GA4 setup incomplete", "red")
        return 1
    
    # Test connection
    connection_ok = test_ga4_connection()
    
    print("\n" + "=" * 60)
    print_colored("TEST SUMMARY", "bold")
    print("=" * 60)
    
    if connection_ok:
        print_colored("✅ GA4 integration is working!", "green")
        print_colored("You can now use GA4 data in traffic attribution", "green")
    else:
        print_colored("❌ GA4 integration needs attention", "red")
    
    return 0 if connection_ok else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_colored("\nTest interrupted by user", "yellow")
        sys.exit(1)
    except Exception as e:
        print_colored(f"\nUnexpected error: {e}", "red")
        import traceback
        traceback.print_exc()
        sys.exit(1)
