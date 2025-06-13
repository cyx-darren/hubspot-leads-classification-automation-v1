
import spam_detector
import quickbooks_domain_updater
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

def check_required_files():
    """Check if required files exist and show clear error messages"""
    print("Checking required files...")
    
    missing_files = []
    
    # Check for leads.csv
    if not os.path.exists('leads.csv'):
        missing_files.append('leads.csv')
        print_colored("✗ leads.csv not found", "red")
    else:
        print_colored("✓ leads.csv found", "green")
    
    # Check for Unique_Email_Domains.csv
    if not os.path.exists('Unique_Email_Domains.csv'):
        missing_files.append('Unique_Email_Domains.csv')
        print_colored("✗ Unique_Email_Domains.csv not found", "red")
    else:
        print_colored("✓ Unique_Email_Domains.csv found", "green")
    
    if missing_files:
        print_colored("\nError: Missing required files:", "red")
        for file in missing_files:
            print_colored(f"  - {file}", "red")
        
        print("\nRequired files:")
        print("  - leads.csv: Contains the email addresses to check for spam")
        print("  - Unique_Email_Domains.csv: Contains whitelisted domains")
        
        if 'Unique_Email_Domains.csv' in missing_files:
            print("\nTo create Unique_Email_Domains.csv:")
            print("  1. Run QuickBooks domain updater separately, or")
            print("  2. Create the file manually with domain names (one per line)")
        
        return False
    
    return True

def ask_user_continue():
    """Ask user if they want to continue with existing whitelist"""
    while True:
        response = input("Continue with existing whitelist? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")

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
            print("  - API authentication issues")
            print("  - Network connectivity problems")
            print("  - QuickBooks API rate limiting")
            print("  - Invalid credentials in Replit Secrets")
            
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

def run_spam_detection():
    """Run spam detection with error handling"""
    print("\nStep 2: Running spam detection...")
    print("-" * 50)
    
    try:
        # Run the spam detection with updated whitelist
        not_spam_count, spam_count = spam_detector.main()
        total_processed = not_spam_count + spam_count
        return not_spam_count, spam_count, total_processed
        
    except KeyboardInterrupt:
        print_colored("\nSpam detection interrupted by user.", "yellow")
        raise
    except FileNotFoundError as e:
        print_colored(f"File not found during spam detection: {e}", "red")
        raise
    except Exception as e:
        print_colored(f"Error during spam detection: {e}", "red")
        raise

def show_final_summary(not_spam_count, spam_count, total_processed, domains_updated=True):
    """Show final summary with results"""
    print("\n" + "=" * 50)
    print("Step 3: Final Summary")
    print("-" * 50)
    
    if domains_updated:
        print_colored("✓ Workflow completed successfully!", "green")
    else:
        print_colored("✓ Spam detection completed (domains not updated)", "yellow")
    
    print(f"📧 Total emails processed: {total_processed}")
    print_colored(f"✅ Not Spam: {not_spam_count} emails (saved to not_spam_leads.csv)", "green")
    print_colored(f"🚫 Spam: {spam_count} emails (saved to spam_leads.csv)", "red")
    
    # Calculate percentages
    if total_processed > 0:
        not_spam_percent = (not_spam_count / total_processed) * 100
        spam_percent = (spam_count / total_processed) * 100
        print(f"📊 Distribution: {not_spam_percent:.1f}% Not Spam, {spam_percent:.1f}% Spam")
    
    print("\n" + "=" * 50)
    print_colored("🎉 Spam detection workflow complete!", "green")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Complete Spam Detection Workflow')
    parser.add_argument('--skip-quickbooks', action='store_true', 
                       help='Skip QuickBooks domain update and run spam detection only')
    args = parser.parse_args()
    
    print_colored("=== Complete Spam Detection Workflow ===\n", "bold")
    
    try:
        # Check for required files first
        if not check_required_files():
            print_colored("\nCannot proceed without required files. Exiting.", "red")
            sys.exit(1)
        
        print_colored("✓ All required files found\n", "green")
        
        domains_updated = True
        
        # Step 1: Update domains (unless skipped)
        if args.skip_quickbooks:
            print_colored("Skipping QuickBooks domain update (--skip-quickbooks flag used)", "yellow")
            print("Using existing Unique_Email_Domains.csv")
            domains_updated = False
        else:
            if not update_domains_with_error_handling():
                sys.exit(1)
            domains_updated = True
        
        print("\n" + "=" * 50)
        
        # Step 2: Run spam detection
        try:
            not_spam_count, spam_count, total_processed = run_spam_detection()
        except KeyboardInterrupt:
            print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
            sys.exit(0)
        except Exception:
            print_colored("\nSpam detection failed. Exiting.", "red")
            sys.exit(1)
        
        # Step 3: Show final summary
        show_final_summary(not_spam_count, spam_count, total_processed, domains_updated)
        
    except KeyboardInterrupt:
        print_colored("\nWorkflow interrupted by user. Exiting.", "yellow")
        sys.exit(0)
    except Exception as e:
        print_colored(f"\nUnexpected error in main workflow: {e}", "red")
        sys.exit(1)

if __name__ == "__main__":
    main()
