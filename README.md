
# HubSpot Automation v1

A modular automation system for processing leads through multiple stages of validation and enrichment.

## Project Structure

```
Hubspot-Automation-v1/
├── modules/
│   ├── __init__.py
│   ├── spam_detector.py           # Module 1: Lead spam detection
│   └── quickbooks_domain_updater.py # Domain whitelist management
├── data/
│   ├── leads.csv                  # Input: Email leads to process
│   └── Unique_Email_Domains.csv   # Whitelist: Trusted domains
├── output/
│   ├── not_spam_leads.csv        # Output: Validated leads (input for next module)
│   └── spam_leads.csv            # Output: Filtered spam leads
├── backups/
│   └── Unique_Email_Domains_backup.csv # Backup of domain whitelist
├── main.py                       # Main workflow orchestrator
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Modules

### 1. Spam Detector ✅ (Complete)
Filters email leads based on:
- Domain whitelist from QuickBooks customers
- Freshdesk ticket history analysis
- Sales team interaction detection

**Input:** `./data/leads.csv`
**Output:** `./output/not_spam_leads.csv`, `./output/spam_leads.csv`

### 2. [Module 2] 🚧 (Placeholder)
*Next automation script will be added here*

**Input:** `./output/not_spam_leads.csv`
**Output:** TBD

### 3. [Module 3] 🚧 (Placeholder)
*Third automation script will be added here*

**Input:** Output from Module 2
**Output:** TBD

## Workflow

1. **Domain Update:** Fetch latest customer domains from QuickBooks
2. **Spam Detection:** Filter leads using whitelist and Freshdesk history
3. **Output Generation:** Separate spam from valid leads
4. **Next Module:** Process valid leads further (coming soon)

## Usage

```bash
# Run complete workflow
python main.py

# Skip QuickBooks update (use existing domains)
python main.py --skip-quickbooks
```

## Setup

1. Configure Replit Secrets:
   - `FRESHDESK_API_KEY`
   - `FRESHDESK_DOMAIN`
   - `QUICKBOOKS_CLIENT_ID`
   - `QUICKBOOKS_CLIENT_SECRET`
   - `QUICKBOOKS_COMPANY_ID`
   - `QUICKBOOKS_REFRESH_TOKEN`

2. Place input file:
   - `./data/leads.csv` - Email addresses to process

3. Run workflow:
   ```bash
   python main.py
   ```

## Output Files

- `./output/not_spam_leads.csv` - Valid leads ready for next processing step
- `./output/spam_leads.csv` - Filtered spam leads for review
- `./backups/Unique_Email_Domains_backup.csv` - Domain whitelist backup

## Future Enhancements

The modular structure supports easy addition of new automation scripts:
- Module 2: Lead enrichment/validation
- Module 3: HubSpot integration/upload
- Additional processing stages as needed

Each module receives structured input and produces structured output for the next stage.
