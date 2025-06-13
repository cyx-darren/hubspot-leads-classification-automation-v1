# main.py
from modules.spam_detector import SpamDetector, main as run_spam_detector
from modules.quickbooks_domain_updater import update_domains

def main():
    # Module selection menu (commented out for now)
    # print("Select a module to run:")
    # print("1. Spam Detector")
    # print("2. QuickBooks Domain Updater")
    # print("3. [Module 3 Placeholder]")
    # choice = input("Enter your choice (1-3): ")

    # if choice == '1':
    print("Running Spam Detector...")
    run_spam_detector()
    # elif choice == '2':
    #     print("Running QuickBooks Domain Updater...")
    #     quickbooks_data = """
    #     john.doe@example.com
    #     jane.smith@test.com
    #     support@newdomain.net
    #     """
    #     update_domains(quickbooks_data)
    # elif choice == '3':
    #     print("Running Module 3...")
    #     # Add code to run module 3 here
    # else:
    #     print("Invalid choice.")

if __name__ == "__main__":
    main()
```

```python
# modules/quickbooks_domain_updater.py
import shutil
from typing import Set
import csv

def read_existing_domains_from_csv(filename: str) -> Set[str]:
    """Reads existing email domains from a CSV file and returns them as a set."""
    domains = set()
    try:
        with open(filename, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:  # Ensure the row is not empty
                    domains.add(row[0].strip().lower())  # Add domain to the set, stripping whitespace and lowercasing
    except FileNotFoundError:
        print(f"Warning: File not found: {filename}. Starting with an empty set of domains.")
    except Exception as e:
        print(f"An error occurred while reading {filename}: {e}")
    return domains

def extract_domains_from_quickbooks(quickbooks_data: str) -> Set[str]:
    """Extracts email domains from raw QuickBooks data (simulated)."""
    # In a real application, this would involve parsing the QuickBooks data.
    # This is a placeholder for that parsing logic.
    emails = [line.strip() for line in quickbooks_data.splitlines() if "@" in line]
    domains = {email.split('@')[1].lower() for email in emails}
    return domains

def merge_domains(existing_domains: Set[str], new_domains: Set[str]) -> Set[str]:
    """Merges existing and new email domains, removing duplicates."""
    all_domains = existing_domains.union(new_domains)
    return all_domains

def save_merged_domains_to_csv(all_domains: Set[str], filename: str = './data/Unique_Email_Domains.csv'):
    """Saves the merged email domains to a CSV file."""
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for domain in sorted(all_domains):  # Sort the domains for better readability
                writer.writerow([domain])
        print(f"Successfully saved {len(all_domains)} domains to {filename}")
    except Exception as e:
        print(f"An error occurred while saving to {filename}: {e}")

def backup_domain_file(filename: str, backup_filename: str):
    """Backs up the domain file before updating."""
    try:
        shutil.copyfile(filename, backup_filename)
        print(f"Successfully backed up {filename} to {backup_filename}")
    except FileNotFoundError:
        print(f"Warning: Could not find {filename} to backup.")
    except Exception as e:
        print(f"An error occurred during backup: {e}")

def update_domains(quickbooks_data: str = None):
    """Main function to update email domains from QuickBooks data."""
    filename = './data/Unique_Email_Domains.csv'
    backup_filename = './backups/Unique_Email_Domains_backup.csv'

    # Backup existing domain file
    backup_domain_file(filename, backup_filename)

    # Read existing domains
    existing_domains = read_existing_domains_from_csv(filename)

    # Extract new domains from QuickBooks (simulated)
    new_domains = extract_domains_from_quickbooks(quickbooks_data or "")

    # Merge domains
    all_domains = merge_domains(existing_domains, new_domains)

    # Save merged domains to CSV
    save_merged_domains_to_csv(all_domains, filename)

if __name__ == '__main__':
    # Simulate QuickBooks data
    quickbooks_data = """
    john.doe@example.com
    jane.smith@test.com
    support@newdomain.net
    """
    update_domains(quickbooks_data)
```

```python
# modules/spam_detector.py
import csv

class SpamDetector:
    def __init__(self, leads_file, whitelist_file, not_spam_output, spam_output):
        self.leads_file = leads_file
        self.whitelist_file = whitelist_file
        self.not_spam_output = not_spam_output
        self.spam_output = spam_output
        self.whitelisted_domains = self.load_whitelist()

    def load_whitelist(self):
        whitelisted_domains = set()
        with open(self.whitelist_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                whitelisted_domains.add(row[0].strip().lower())
        return whitelisted_domains

    def analyze_lead(self, email):
        domain = email.split('@')[1].lower()
        return domain in self.whitelisted_domains

    def process_leads(self):
        not_spam_leads = []
        spam_leads = []

        with open(self.leads_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header

            for row in reader:
                email = row[0]
                if self.analyze_lead(email):
                    not_spam_leads.append(row)
                else:
                    spam_leads.append(row)

        self.save_leads(not_spam_leads, self.not_spam_output)
        self.save_leads(spam_leads, self.spam_output)

    def save_leads(self, leads, output_file):
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            #writer.writerow(['Email', 'Other', 'Fields']) #Example header
            writer.writerows(leads)

def main():
    leads_file = "./data/leads.csv"
    whitelist_file = "./data/Unique_Email_Domains.csv"
    not_spam_output = "./output/not_spam_leads.csv"
    spam_output = "./output/spam_leads.csv"

    detector = SpamDetector(leads_file, whitelist_file, not_spam_output, spam_output)
    detector.process_leads()

if __name__ == "__main__":
    main()
```

```markdown
# README.md
# HubSpot Automation v1

## Modules:
1. Spam Detector - Filters leads based on Freshdesk history
2. [Placeholder for next script]
3. [Placeholder for third script]

## Workflow:
1. Update customer domains from QuickBooks
2. Run spam detection on leads
3. Output: not_spam_leads.csv (input for next module)
```

```python
# modules/__init__.py