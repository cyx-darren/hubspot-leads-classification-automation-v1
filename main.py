
import spam_detector

def main():
    print("Starting spam detection...")
    
    # Run the spam detection
    not_spam_count, spam_count = spam_detector.main()
    
    print("Spam detection complete")
    print(f"Not Spam: {not_spam_count} emails (saved to not_spam_leads.csv)")
    print(f"Spam: {spam_count} emails (saved to spam_leads.csv)")

if __name__ == "__main__":
    main()
