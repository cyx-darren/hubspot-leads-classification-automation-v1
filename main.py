
import spam_detector
import quickbooks_domain_updater
import sys

def main():
    print("=== Complete Spam Detection Workflow ===\n")
    
    # Step 1: Update domains from QuickBooks
    print("Step 1: Checking for new customer domains...")
    print("-" * 50)
    
    try:
        # Run QuickBooks domain updater
        exit_code = quickbooks_domain_updater.main()
        
        if exit_code != 0:
            print("Warning: QuickBooks domain update failed, continuing with existing domains...")
            new_domains_count = 0
        else:
            # Try to extract the number of new domains from the output
            # This is a simple approach - in a real implementation you might want
            # to modify quickbooks_domain_updater to return this information
            new_domains_count = "N/A"  # We'll update this to show actual count if available
            
    except Exception as e:
        print(f"Error updating domains: {e}")
        print("Continuing with existing domains...")
        new_domains_count = 0
    
    print("\n" + "=" * 50)
    
    # Step 2: Run spam detection
    print("\nStep 2: Running spam detection...")
    print("-" * 50)
    
    try:
        # Run the spam detection with updated whitelist
        not_spam_count, spam_count = spam_detector.main()
        total_processed = not_spam_count + spam_count
        
    except Exception as e:
        print(f"Error during spam detection: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 50)
    
    # Step 3: Show final summary
    print("\nStep 3: Final Summary")
    print("-" * 50)
    print(f"✓ Workflow completed successfully!")
    print(f"📧 Total emails processed: {total_processed}")
    print(f"✅ Not Spam: {not_spam_count} emails (saved to not_spam_leads.csv)")
    print(f"🚫 Spam: {spam_count} emails (saved to spam_leads.csv)")
    
    # Calculate percentages
    if total_processed > 0:
        not_spam_percent = (not_spam_count / total_processed) * 100
        spam_percent = (spam_count / total_processed) * 100
        print(f"📊 Distribution: {not_spam_percent:.1f}% Not Spam, {spam_percent:.1f}% Spam")
    
    print("\n" + "=" * 50)
    print("🎉 Spam detection workflow complete!")

if __name__ == "__main__":
    main()
