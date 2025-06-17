import glob
from modules import spam_detector
from modules import quickbooks_domain_updater
import sys
import os
import argparse

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

def find_leads_file():
    """Find any leads file in data/ directory or root"""
    # Look for leads files in data/ directory first, then root
    patterns = [
        './data/leads*.csv',
        './data/Leads*.csv',
        'leads*.csv',
        'Leads*.csv'
    ]

    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            # Return the most recent file if multiple found
            return max(files, key=os.path.getmtime)

    return None

def check_required_files(leads_file):
    """Check if required files exist and show clear error messages"""
    print("Checking required files...")

    missing_files = []

    # Check for leads file
    if not leads_file or not os.path.exists(leads_file):
        missing_files.append("leads file")
        print_colored("✗ No leads file found", "red")
    else:
        print_colored(f"✓ Found leads file: {leads_file}", "green")

    # Check for Unique_Email_Domains.csv
    if not os.path.exists('./data/Unique_Email_Domains.csv'):
        missing_files.append('./data/Unique_Email_Domains.csv')
        print_colored("✗ ./data/Unique_Email_Domains.csv not found", "red")
    else:
        print_colored("✓ ./data/Unique_Email_Domains.csv found", "green")

    # Check for Product_Catalogue.csv (optional for lead analysis)
    if not os.path.exists('./data/Product_Catalogue.csv'):
        print_colored("⚠ ./data/Product_Catalogue.csv not found (optional for lead analysis)", "yellow")
    else:
        print_colored("✓ ./data/Product_Catalogue.csv found", "green")

    if missing_files:
        print_colored("\nError: Missing required files:", "red")
        for file in missing_files:
            print_colored(f"  - {file}", "red")

        if "leads file" in missing_files:
            print("\nPlease ensure you have a leads file:")
            print("  - Named like: leads.csv, leads_may2025.csv, leads_dec2024.csv, etc.")
            print("  - Located in ./data/ directory or root directory")

        if './data/Unique_Email_Domains.csv' in missing_files:
            print("\nTo create ./data/Unique_Email_Domains.csv:")
            print("  1. Run QuickBooks domain updater separately, or")
            print("  2. Create the file manually with domain names (one per line)")

        return False

    return True

def ask_user_continue():
    """Ask user if they want to continue with existing whitelist"""
    while True:
        response = input("Continue with existing whitelist? (Y/n): ").strip().lower()
        if response in ['y', 'yes', '']:  # Default to yes if user just presses Enter
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no (default: yes).")

def update_domains_with_error_handling():
    """Update domains from QuickBooks with comprehensive error handling"""
    print("Step 1: Checking for new customer domains...")
    print("-" * 50)

    try:
        # Run QuickBooks domain updater
        exit_code = quickbooks_domain_updater.main()

        if exit_code != 0:
            print_colored("\nWarning: QuickBooks domain update failed!", "yellow")
            print("This could be due to:")
            print("  - Expired refresh token (most common - expires after ~100 days)")
            print("  - API authentication issues")
            print("  - Network connectivity problems")
            print("  - QuickBooks API rate limiting")
            print("  - Invalid credentials in Replit Secrets")
            print("\nTip: Use --skip-quickbooks flag to skip this step next time:")

            if not ask_user_continue():
                print_colored("Exiting as requested by user.", "yellow")
                return False

            print_colored("Continuing with existing whitelist...", "green")
        else:
            print_colored("✓ Domain update completed successfully!", "green")

    except KeyboardInterrupt:
        print_colored("\nProcess interrupted by user.", "yellow")
        return False
    except Exception as e:
        print_colored(f"\nError during QuickBooks update: {e}", "red")
        print("This could be due to:")
        print("  - Missing or invalid API credentials")
        print("  - Network connectivity issues")
        print("  - QuickBooks service unavailability")

        if not ask_user_continue():
            print_colored("Exiting as requested by user.", "yellow")
            return False

        print_colored("Continuing with existing whitelist...", "green")

    return True

def run_spam_detection(leads_file):
    """Run spam detection with error handling"""
    print("\nStep 2: Running spam detection...")
    print("-" * 50)

    try:
        # Create SpamDetector with filename for date detection
        # Extract just the filename for date parsing
        filename_for_parsing = os.path.basename(leads_file)
        detector = spam_detector.SpamDetector(filename=filename_for_parsing)

        print_colored(f"\n=== Spam Detector with Dynamic Date Detection ===", spam_detector.Colors.BOLD + spam_detector.Colors.BLUE)
        print_colored(f"Analyzing tickets from {detector.start_date.strftime('%B %d, %Y')} to {detector.end_date.strftime('%B %d, %Y')}", spam_detector.Colors.BLUE)

        # Check if API keys are set
        if not spam_detector.FRESHDESK_API_KEY:
            print_colored("Error: FRESHDESK_API_KEY environment variable not set", spam_detector.Colors.RED)
            sys.exit(1)
        if not spam_detector.FRESHDESK_DOMAIN:
            print_colored("Error: FRESHDESK_DOMAIN environment variable not set", spam_detector.Colors.RED)
            sys.exit(1)

        # Path to whitelist file
        whitelist_file = "./data/Unique_Email_Domains.csv"

        # Read whitelist
        print(f"\nReading whitelist from {whitelist_file}...")
        whitelisted_domains = detector.read_whitelist(whitelist_file)
        if whitelisted_domains:
            print_colored(f"Successfully loaded {len(whitelisted_domains)} whitelisted domains", spam_detector.Colors.GREEN)
        else:
            print_colored("Warning: No whitelisted domains found", spam_detector.Colors.YELLOW)

        # Read emails from the specified leads file
        print(f"Reading emails from {leads_file}...")
        emails_to_check = detector.read_emails_from_file(leads_file)

        if emails_to_check:
            print_colored(f"Successfully loaded {len(emails_to_check)} emails to check", spam_detector.Colors.GREEN)
        else:
            print_colored(f"No valid emails found in {leads_file}. Exiting.", spam_detector.Colors.RED)
            sys.exit(1)

        # Process each email
        print_colored(f"\nProcessing emails for tickets from {detector.start_date.strftime('%B %Y')} to {detector.end_date.strftime('%B %Y')}...", spam_detector.Colors.BLUE)
        results = []

        for i, email in enumerate(emails_to_check):
            progress = f"[{i+1}/{len(emails_to_check)}]"
            print(f"\n{progress} Processing: {email}")

            classification_result = detector.classify_email(email, whitelisted_domains)
            results.append(classification_result)

            if classification_result['classification'] == 'Spam':
                status_color = spam_detector.Colors.RED
            else:
                status_color = spam_detector.Colors.GREEN

            print_colored(f"{progress} Result: {classification_result['classification']} - {classification_result['reason']}", status_color)

            # Print more details for debugging if spam with ticket history
            if classification_result['classification'] == 'Spam' and 'ticket_count' in classification_result['details'] and classification_result['details']['ticket_count'] > 0:
                print("  Detailed check results:")
                if 'sales_checks' in classification_result['details']:
                    for check in classification_result['details']['sales_checks']:
                        print(f"  - Ticket {check['ticket_id']} (created: {check['created_at']}): {check['details']}")

        # Save results
        not_spam_count, spam_count = detector.save_results_to_csv(results)

        print_colored("\n=== Spam Detection Summary ===", spam_detector.Colors.BOLD + spam_detector.Colors.BLUE)
        print_colored(f"Analysis Period: {detector.start_date.strftime('%B %d, %Y')} - {detector.end_date.strftime('%B %d, %Y')}", spam_detector.Colors.BLUE)
        print(f"Total emails processed: {len(results)}")
        print(f"Spam emails detected: {spam_count}")
        print(f"Non-spam emails detected: {not_spam_count}")

        return not_spam_count, spam_count, len(results), detector

    except KeyboardInterrupt:
        print_colored("\nSpam detection interrupted by user.", "yellow")
        raise
    except FileNotFoundError as e:
        print_colored(f"File not found during spam detection: {e}", "red")
        raise
    except Exception as e:
        print_colored(f"Error during spam detection: {e}", "red")
        raise

def run_lead_analysis(start_date=None, end_date=None):
    """Run lead analysis on not_spam leads"""
    print("Step 3: Analyzing lead products and Freshdesk data...")
    print("-" * 50)

    try:
        from modules import lead_analyzer

        # Check if not_spam_leads.csv exists
        input_file = "./output/not_spam_leads.csv"
        if not os.path.exists(input_file):
            print_colored(f"Warning: {input_file} not found. Skipping lead analysis.", "yellow")
            return 0

        # Check if Product_Catalogue.csv exists
        catalog_file = "./data/Product_Catalogue.csv"
        if not os.path.exists(catalog_file):
            print_colored(f"Warning: {catalog_file} not found. Analysis will be limited.", "yellow")

        # Use the same date range as spam detector, or defaults if not provided
        success = lead_analyzer.analyze_leads(
            input_csv_path="./output/not_spam_leads.csv",
            output_csv_path="./output/leads_with_products.csv",
            start_date=start_date,
            end_date=end_date
        )

        if success:
            print_colored("✓ Lead analysis completed successfully!", "green")
            # Count analyzed leads
            try:
                import pandas as pd
                df = pd.read_csv("./output/leads_with_products.csv")
                return len(df)
            except:
                return 0
        else:
            print_colored("✗ Lead analysis failed", "red")
            return 0

    except ImportError as e:
        print_colored(f"Error importing lead analyzer: {e}", "red")
        return 0
    except Exception as e:
        print_colored(f"Error during lead analysis: {e}", "red")
        return 0

def run_traffic_attribution():
    """Run traffic attribution analysis on leads with products"""
    print("\n" + "="*50)
    print("STEP 4: Running Traffic Attribution Analysis")
    print("="*50)

    # Check Google Search Console integration
    print("\nChecking Google Search Console integration...")
    try:
        from modules.gsc_client import get_gsc_credentials
        if get_gsc_credentials():
            print_colored("✓ GSC credentials found - will use real search data", "green")
        else:
            print_colored("ℹ️  GSC not configured - using ranking data only", "blue")
            print_colored("  To enable: Add GSC_CREDENTIALS to Secrets or upload credentials file", "blue")
    except Exception as e:
        print_colored(f"Warning: Could not check GSC status: {e}", "yellow")

    try:
        from modules.traffic_attribution import analyze_traffic_attribution

        # Check if prerequisite file exists
        if not os.path.exists("output/leads_with_products.csv"):
            print_colored("Warning: leads_with_products.csv not found. Skipping attribution.", "yellow")
            return 0

        # Check for GSC credentials
        gsc_creds_path = "data/gsc_credentials.json"
        use_gsc = os.path.exists(gsc_creds_path)
        gsc_property_url = os.environ.get('GSC_PROPERTY_URL')

        # Check for GA4 integration
        use_ga4 = bool(os.environ.get('GA4_PROPERTY_ID'))

        if use_gsc:
            print_colored("✓ GSC credentials found - using real search click data", "green")
            if gsc_property_url:
                print_colored(f"  Property URL: {gsc_property_url}", "blue")
            else:
                print_colored("  Note: GSC_PROPERTY_URL not set in secrets - will try to auto-detect", "blue")
        else:
            print_colored("ℹ️  GSC credentials not found - using ranking data only", "blue")
            print_colored("  Tip: Setup GSC integration for enhanced SEO attribution", "blue")
            print_colored("  See data/gsc_setup.md for instructions", "blue")

        if use_ga4:
            print_colored("✓ GA4 integration detected - will validate attributions", "green")
        else:
            print_colored("ℹ️  GA4 not configured - attribution validation disabled", "blue")

        # Define data file paths (these should exist in the data/ directory)
        seo_path = "data/Feb2025-SEO.csv"
        ppc_standard_path = "data/When your ads showed Custom and Corporate Gifts and Lanyards (1).csv"
        ppc_dynamic_path = "data/When your ads showed Dynamic Search Ads (1).csv"

        # Check if data files exist, use None if not found
        if not os.path.exists(seo_path):
            print_colored(f"Warning: SEO data not found at {seo_path}", "yellow")
            seo_path = None

        if not os.path.exists(ppc_standard_path):
            print_colored(f"Warning: PPC standard data not found at {ppc_standard_path}", "yellow")
            ppc_standard_path = None

        if not os.path.exists(ppc_dynamic_path):
            print_colored(f"Warning: PPC dynamic data not found at {ppc_dynamic_path}", "yellow")
            ppc_dynamic_path = None

        # Run attribution
        try:
            result = analyze_traffic_attribution(
                leads_path="output/leads_with_products.csv",
                seo_csv_path=seo_path,
                ppc_standard_path=ppc_standard_path,
                ppc_dynamic_path=ppc_dynamic_path,
                output_path="output/leads_with_attribution.csv",
                use_gsc=use_gsc,
                gsc_credentials_path=gsc_creds_path if use_gsc else None,
                gsc_property_url=gsc_property_url,
                use_ga4=use_ga4
            )

            if result > 0:
                print_colored(f"✓ Successfully attributed {result} leads", "green")
                print_colored("✓ Results saved to output/leads_with_attribution.csv", "green")
                return result
            else:
                print_colored("✗ No leads were attributed", "red")
                return 0

        except Exception as e:
            print_colored(f"✗ Error during attribution: {e}", "red")
            return 0

    except ImportError as e:
        print_colored(f"Error importing traffic attribution: {e}", "red")
        return 0
    except Exception as e:
        print_colored(f"Error during traffic attribution: {e}", "red")
        return 0

def show_final_summary(not_spam_count, spam_count, total_processed, analyzed_leads_count, attributed_leads_count, domains_updated, leads_file):
    """Show final workflow summary"""
    print_colored("\n" + "=" * 60, "bold")
    print_colored("🎉 HUBSPOT AUTOMATION v1 - WORKFLOW COMPLETE! 🎉", "bold")
    print_colored("=" * 60, "bold")

    print(f"\n📊 PROCESSING SUMMARY:")
    print(f"   Input file: {leads_file}")
    print(f"   Total leads processed: {total_processed}")
    print(f"   ✅ Valid leads: {not_spam_count}")
    print(f"   🚫 Spam leads filtered: {spam_count}")
    print(f"   📈 Leads analyzed: {analyzed_leads_count}")
    print(f"   🎯 Attribution completed: {attributed_leads_count}")

    if domains_updated:
        print(f"\n🔄 DOMAIN UPDATES:")
        print(f"   ✅ QuickBooks domains refreshed")

    print(f"\n📁 OUTPUT FILES GENERATED:")
    print(f"   📋 Clean leads: ./output/not_spam_leads.csv ({not_spam_count} leads)")
    print(f"   🗑️  Spam leads: ./output/spam_leads.csv ({spam_count} leads)")
    if analyzed_leads_count > 0:
        print(f"   🔍 Product analysis: ./output/leads_with_products.csv ({analyzed_leads_count} leads)")
    if attributed_leads_count > 0:
        print(f"   🎯 Traffic attribution: ./output/leads_with_attribution.csv ({attributed_leads_count} leads)")

    print(f"\n🚀 NEXT STEPS:")
    print(f"   1. Review the clean leads in ./output/not_spam_leads.csv")
    if analyzed_leads_count > 0:
        print(f"   2. Check product insights in ./output/leads_with_products.csv")
    if attributed_leads_count > 0:
        print(f"   3. Analyze traffic sources in ./output/leads_with_attribution.csv")
    print(f"   4. Import clean leads to HubSpot (Module 3 - coming soon)")

    print_colored("\n" + "=" * 60, "bold")
    print_colored("✨ Ready for next automation module! ✨", "green")
    print_colored("💡 Tip: Run again with --skip-quickbooks to save time", "blue")
    print_colored("📧 Support: Check README.md for troubleshooting", "blue")
    print_colored("=" * 60, "bold")

    # Show file locations for easy access
    print(f"\n📍 Quick access to your files:")
    print(f"   Clean leads ready for import: ./output/not_spam_leads.csv ({not_spam_count} clean leads)")
    if attributed_leads_count > 0:
        print(f"   Full analysis with attribution: ./output/leads_with_attribution.csv ({attributed_leads_count} analyzed leads)")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='HubSpot Automation v1 - Complete Workflow')
    parser.add_argument('--skip-quickbooks', action='store_true', 
                       help='Skip QuickBooks domain update and run spam detection only')
    parser.add_argument('--input', help='Input leads CSV file', default=None)
    args = parser.parse_args()

    print_colored("=== HubSpot Automation v1 - Complete Workflow ===\n", "bold")

    try:
        # Find leads file
        if args.input:
            leads_file = args.input
            if not os.path.exists(leads_file):
                print_colored(f"✗ Specified input file not found: {leads_file}", "red")
                sys.exit(1)
        else:
            leads_file = find_leads_file()

            if not leads_file:
                print_colored("✗ No leads file found", "red")
                print("\nPlease ensure you have a leads file:")
                print("  - Named like: leads.csv, leads_may2025.csv, leads_dec2024.csv, etc.")
                print("  - Located in ./data/ directory or root directory")
                print("\nAlternatively, specify a file with --input:")
                print("  python main.py --input your_leads_file.csv")
                sys.exit(1)

        # Check for required files
        if not check_required_files(leads_file):
            print_colored("\nCannot proceed without required files. Exiting.", "red")
            sys.exit(1)

        print_colored("✓ All required files found\n", "green")

        domains_updated = True

        # Step 1: Update domains (unless skipped)
        if args.skip_quickbooks:
            print_colored("Skipping QuickBooks domain update (--skip-quickbooks flag used)", "yellow")
            print("Using existing ./data/Unique_Email_Domains.csv")
            domains_updated = False
        else:
            if not update_domains_with_error_handling():
                sys.exit(1)
            domains_updated = True

        print("\n" + "=" * 50)

        # Step 2: Run spam detection
        detector = None
        try:
            not_spam_count, spam_count, total_processed, detector = run_spam_detection(leads_file)
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nSpam detection failed. Exiting.", "red")
            sys.exit(1)

        print("\n" + "=" * 50)

        # Step 3: Run lead analysis with same date range as spam detector
        analyzed_leads_count = 0
        try:
            if detector:
                analyzed_leads_count = run_lead_analysis(detector.start_date, detector.end_date)
            else:
                analyzed_leads_count = run_lead_analysis()
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nLead analysis failed, but continuing to summary.", "yellow")

        # Step 4: Run traffic attribution
        attributed_leads_count = 0
        try:
            attributed_leads_count = run_traffic_attribution()
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nTraffic attribution failed, but continuing to summary.", "yellow")

        # Step 5: Show final summary
        show_final_summary(not_spam_count, spam_count, total_processed, analyzed_leads_count, attributed_leads_count, domains_updated, leads_file)

    except KeyboardInterrupt:
        print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
        sys.exit(0)
    except Exception as e:
        print_colored(f"\nUnexpected error in main workflow: {e}", "red")
        sys.exit(1)

def run_enhanced_attribution_analysis():
    """Run enhanced attribution analysis with email content analysis"""
    print_colored("\n3. RUNNING ENHANCED ATTRIBUTION ANALYSIS", "blue")
    print_colored("-" * 50, "blue")

    # Check if leads_with_products.csv exists
    if not os.path.exists("output/leads_with_products.csv"):
        print_colored("Error: leads_with_products.csv not found. Run lead analysis first.", "red")
        return False

    try:
        from modules.traffic_attribution import analyze_traffic_attribution

        # GSC configuration check
        use_gsc = bool(os.environ.get('GSC_CREDENTIALS')) or os.path.exists('data/gsc_credentials.json')
        gsc_property_url = os.environ.get('GSC_PROPERTY_URL')

        # GA4 configuration check  
        use_ga4 = bool(os.environ.get('GA4_PROPERTY_ID'))

        print(f"✓ Enhanced attribution with email content analysis enabled")
        print(f"✓ GSC integration {'enabled' if use_gsc else 'disabled'}")
        if use_ga4:
            print("✓ GA4 integration enabled")
        else:
            print("ℹ️  GA4 not configured")

        result = analyze_traffic_attribution(
            leads_path="output/leads_with_products.csv",
            seo_csv_path="data/Feb2025-SEO.csv",
            ppc_standard_path="data/When your ads showed Custom and Corporate Gifts and Lanyards (1).csv",
            ppc_dynamic_path="data/When your ads showed Dynamic Search Ads (1).csv",
            output_path="output/leads_with_enhanced_attribution.csv",
            use_gsc=use_gsc,
            gsc_credentials_path = 'data/gsc_credentials.json' if use_gsc else None,
            gsc_property_url=gsc_property_url,
            use_ga4=use_ga4
        )

        print(f"\n✓ Enhanced attribution analysis completed: {result} leads processed")
        print_colored("✓ Results saved to output/leads_with_enhanced_attribution.csv", "green")
        print_colored("✓ Enhanced columns: drill_down, email_content_override, override_reason", "green")

    except Exception as e:
        print(f"\n✗ Error during enhanced attribution analysis: {e}")
        return False

    return True

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='HubSpot Automation v1 - Complete Workflow')
    parser.add_argument('--skip-quickbooks', action='store_true', 
                       help='Skip QuickBooks domain update and run spam detection only')
    parser.add_argument('--input', help='Input leads CSV file', default=None)
    args = parser.parse_args()

    print_colored("=== HubSpot Automation v1 - Complete Workflow ===\n", "bold")

    try:
        # Find leads file
        if args.input:
            leads_file = args.input
            if not os.path.exists(leads_file):
                print_colored(f"✗ Specified input file not found: {leads_file}", "red")
                sys.exit(1)
        else:
            leads_file = find_leads_file()

            if not leads_file:
                print_colored("✗ No leads file found", "red")
                print("\nPlease ensure you have a leads file:")
                print("  - Named like: leads.csv, leads_may2025.csv, leads_dec2024.csv, etc.")
                print("  - Located in ./data/ directory or root directory")
                print("\nAlternatively, specify a file with --input:")
                print("  python main.py --input your_leads_file.csv")
                sys.exit(1)

        # Check for required files
        if not check_required_files(leads_file):
            print_colored("\nCannot proceed without required files. Exiting.", "red")
            sys.exit(1)

        print_colored("✓ All required files found\n", "green")

        domains_updated = True

        # Step 1: Update domains (unless skipped)
        if args.skip_quickbooks:
            print_colored("Skipping QuickBooks domain update (--skip-quickbooks flag used)", "yellow")
            print("Using existing ./data/Unique_Email_Domains.csv")
            domains_updated = False
        else:
            if not update_domains_with_error_handling():
                sys.exit(1)
            domains_updated = True

        print("\n" + "=" * 50)

        # Step 2: Run spam detection
        detector = None
        try:
            not_spam_count, spam_count, total_processed, detector = run_spam_detection(leads_file)
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nSpam detection failed. Exiting.", "red")
            sys.exit(1)

        print("\n" + "=" * 50)

        # Step 3: Run lead analysis with same date range as spam detector
        analyzed_leads_count = 0
        try:
            if detector:
                analyzed_leads_count = run_lead_analysis(detector.start_date, detector.end_date)
            else:
                analyzed_leads_count = run_lead_analysis()
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nLead analysis failed, but continuing to summary.", "yellow")

        # Step 4: Run traffic attribution
        attributed_leads_count = 0
        try:
            attributed_leads_count = run_traffic_attribution()
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nTraffic attribution failed, but continuing to summary.", "yellow")

        # Step 5: Show final summary
        show_final_summary(not_spam_count, spam_count, total_processed, analyzed_leads_count, attributed_leads_count, domains_updated, leads_file)
```

This code adds an option to run enhanced attribution analysis and includes the corresponding function definition.