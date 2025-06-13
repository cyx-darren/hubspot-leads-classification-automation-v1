
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
        
        return not_spam_count, spam_count, len(results)
        
    except KeyboardInterrupt:
        print_colored("\nSpam detection interrupted by user.", "yellow")
        raise
    except FileNotFoundError as e:
        print_colored(f"File not found during spam detection: {e}", "red")
        raise
    except Exception as e:
        print_colored(f"Error during spam detection: {e}", "red")
        raise

def show_final_summary(not_spam_count, spam_count, total_processed, domains_updated=True, leads_file=""):
    """Show final summary with results"""
    print("\n" + "=" * 50)
    print("Step 3: Final Summary")
    print("-" * 50)
    
    if domains_updated:
        print_colored("✓ Workflow completed successfully!", "green")
    else:
        print_colored("✓ Spam detection completed (domains not updated)", "yellow")
    
    print(f"📁 Input file: {leads_file}")
    print(f"📧 Total emails processed: {total_processed}")
    print_colored(f"✅ Not Spam: {not_spam_count} emails (saved to ./output/not_spam_leads.csv)", "green")
    print_colored(f"🚫 Spam: {spam_count} emails (saved to ./output/spam_leads.csv)", "red")
    
    # Calculate percentages
    if total_processed > 0:
        not_spam_percent = (not_spam_count / total_processed) * 100
        spam_percent = (spam_count / total_processed) * 100
        print(f"📊 Distribution: {not_spam_percent:.1f}% Not Spam, {spam_percent:.1f}% Spam")
    
    print("\n" + "=" * 50)
    print_colored("🎉 Spam detection workflow complete!", "green")
    
    # Placeholder for future modules
    print_colored("\n📋 Next Steps (Future Modules):", "blue")
    print("  Module 2: [Placeholder for next automation script]")
    print("  Module 3: [Placeholder for third automation script]")
    print(f"  Input for next module: ./output/not_spam_leads.csv ({not_spam_count} emails)")

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
        try:
            not_spam_count, spam_count, total_processed = run_spam_detection(leads_file)
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nSpam detection failed. Exiting.", "red")
            sys.exit(1)
        
        # Step 3: Show final summary
        show_final_summary(not_spam_count, spam_count, total_processed, domains_updated, leads_file)
        
    except KeyboardInterrupt:
        print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
        sys.exit(0)
    except Exception as e:
        print_colored(f"\nUnexpected error in main workflow: {e}", "red")
        sys.exit(1)

if __name__ == "__main__":
    main()
