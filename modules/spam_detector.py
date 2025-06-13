import csv
from enum import Enum

# Define a simple color class for colored output
class Colors(Enum):
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'

# Function for colored output
def print_colored(text, color: Colors):
    print(f"{color.value}{text}{Colors.RESET.value}")

# Function to read emails from a file
def read_emails_from_file(file_path):
    emails = []
    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip the header
            for row in reader:
                if row:
                    emails.append(row[0].strip())  # Assuming email is the first column
    except FileNotFoundError:
        print_colored(f"Error: File not found at {file_path}", Colors.RED)
    except Exception as e:
        print_colored(f"An error occurred: {e}", Colors.RED)
    return emails

# Function to read the whitelist from a file
def read_whitelist(file_path):
    domains = set()
    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip the header
            for row in reader:
                if row:
                    domains.add(row[0].strip())  # Assuming domain is the first column
    except FileNotFoundError:
        print_colored(f"Error: Whitelist file not found at {file_path}", Colors.RED)
    except Exception as e:
        print_colored(f"An error occurred: {e}", Colors.RED)
    return domains

# Function to check emails against the whitelist
def check_emails_against_whitelist(emails, whitelisted_domains):
    not_spam = []
    spam = []
    for email in emails:
        domain = email.split('@')[1] if '@' in email else None
        if domain and domain in whitelisted_domains:
            not_spam.append(email)
        else:
            spam.append(email)
    return not_spam, spam

# Main function
def main():
    # Path to whitelist file
    whitelist_file = "./data/Unique_Email_Domains.csv"

    # Read whitelist
    print(f"\nReading whitelist from {whitelist_file}...")
    whitelisted_domains = read_whitelist(whitelist_file)
    if whitelisted_domains:
        print_colored(f"Successfully loaded {len(whitelisted_domains)} whitelisted domains", Colors.GREEN)
    else:
        print_colored("Warning: No whitelisted domains found", Colors.YELLOW)

    # Automatically read from leads.csv
    email_file = "./data/leads.csv"
    print(f"Reading emails from {email_file}...")
    emails_to_check = read_emails_from_file(email_file)

    # Check emails against the whitelist
    not_spam_results, spam_results = check_emails_against_whitelist(emails_to_check, whitelisted_domains)

    # Save not spam emails
    try:
        with open('./output/not_spam_leads.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['email'])  # Header
            for email in not_spam_results:
                writer.writerow([email])
    except Exception as e:
        print_colored(f"Error writing to file: {e}", Colors.RED)

    # Save spam emails
    try:
        with open('./output/spam_leads.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['email'])  # Header
            for email in spam_results:
                writer.writerow([email])
    except Exception as e:
        print_colored(f"Error writing to file: {e}", Colors.RED)

    print_colored(f"Not spam results saved to ./output/not_spam_leads.csv ({len(not_spam_results)} emails)", Colors.GREEN)
    print_colored(f"Spam results saved to ./output/spam_leads.csv ({len(spam_results)} emails)", Colors.GREEN)

# Execute main function
if __name__ == "__main__":
    main()