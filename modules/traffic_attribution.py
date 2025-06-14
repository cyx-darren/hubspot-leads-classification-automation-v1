
#!/usr/bin/env python
"""
Traffic Attribution Module for HubSpot Automation v1

This module analyzes lead sources to determine whether they came from 
SEO, Google Ads PPC, direct traffic, or referrals.
Integrated with existing QuickBooks and Freshdesk data.
"""

import os
import re
import logging
import datetime
import warnings
from collections import defaultdict
from dateutil import parser
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('traffic_attribution')

# Import fuzzy matching if available
try:
    from fuzzywuzzy import fuzz, process
    FUZZY_AVAILABLE = True
except ImportError:
    logger.warning("fuzzywuzzy not available - using basic string matching")
    FUZZY_AVAILABLE = False

# Color codes for better terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_colored(text: str, color: str):
    """Print text with color for better readability"""
    print(f"{color}{text}{Colors.ENDC}")

class LeadAttributionAnalyzer:
    def __init__(self):
        self.leads_df = None
        self.customers_df = None
        self.seo_keywords_df = None
        self.ppc_standard_df = None
        self.ppc_dynamic_df = None
        self.combined_ppc_df = None
        self.product_keyword_map = None
        self.attribution_window_hours = 48
        self.confidence_thresholds = {
            'high': 80,
            'medium': 50,
            'low': 20
        }

    def load_data(self, 
                 leads_path="./output/leads_with_products.csv", 
                 seo_csv_path=None, 
                 ppc_standard_path=None, 
                 ppc_dynamic_path=None):
        """Load all data sources"""
        print_colored("Loading data sources for attribution analysis...", Colors.BLUE)

        # Load leads data from lead_analyzer output
        try:
            self.leads_df = pd.read_csv(leads_path)
            print_colored(f"✓ Loaded {len(self.leads_df)} leads from {leads_path}", Colors.GREEN)
        except FileNotFoundError:
            print_colored(f"Error: Leads file not found: {leads_path}", Colors.RED)
            raise
        except Exception as e:
            print_colored(f"Error loading leads data: {e}", Colors.RED)
            raise

        # Load customer data from QuickBooks
        try:
            self.customers_df = self.load_customers_from_quickbooks()
            print_colored(f"✓ Loaded {len(self.customers_df)} customer records from QuickBooks", Colors.GREEN)
        except Exception as e:
            print_colored(f"Warning: Could not load QuickBooks customers: {e}", Colors.YELLOW)
            # Create empty DataFrame as fallback
            self.customers_df = pd.DataFrame(columns=['email'])

        # Load SEO data if provided
        if seo_csv_path and os.path.exists(seo_csv_path):
            self.seo_keywords_df = self.load_seo_data_from_csv(seo_csv_path)
            print_colored(f"✓ Loaded {len(self.seo_keywords_df)} SEO keywords", Colors.GREEN)
        else:
            print_colored("No SEO data provided - creating mock data for analysis", Colors.YELLOW)
            self.seo_keywords_df = self.create_mock_seo_data()

        # Load PPC data if provided
        if ppc_standard_path and ppc_dynamic_path:
            try:
                self.ppc_standard_df = pd.read_csv(ppc_standard_path)
                self.ppc_dynamic_df = pd.read_csv(ppc_dynamic_path)
                print_colored(f"✓ Loaded PPC data: {len(self.ppc_standard_df)} standard + {len(self.ppc_dynamic_df)} dynamic records", Colors.GREEN)
            except Exception as e:
                print_colored(f"Warning: Could not load PPC data: {e}", Colors.YELLOW)
                self.ppc_standard_df = pd.DataFrame()
                self.ppc_dynamic_df = pd.DataFrame()
        else:
            print_colored("No PPC data provided - creating mock data for analysis", Colors.YELLOW)
            self.ppc_standard_df = pd.DataFrame()
            self.ppc_dynamic_df = pd.DataFrame()

        # Process and clean the data
        self.process_data()

    def load_customers_from_quickbooks(self) -> pd.DataFrame:
        """Load customer emails from QuickBooks API using existing integration"""
        try:
            # Import QuickBooks functionality from existing module
            from modules.quickbooks_domain_updater import get_quickbooks_customers, extract_customer_domains
            
            print_colored("Fetching customers from QuickBooks API...", Colors.BLUE)
            
            # Get customers from QuickBooks
            customers = get_quickbooks_customers()
            
            if not customers:
                print_colored("No customers retrieved from QuickBooks", Colors.YELLOW)
                return pd.DataFrame(columns=['email'])
            
            # Extract email addresses from customers
            customer_emails = []
            for customer in customers:
                email = customer.get('PrimaryEmailAddr', {}).get('Address', '')
                if email and '@' in email:
                    customer_emails.append(email.lower().strip())
            
            # Create DataFrame
            customers_df = pd.DataFrame({
                'email': customer_emails
            })
            
            # Remove duplicates
            customers_df = customers_df.drop_duplicates()
            
            return customers_df
            
        except ImportError:
            print_colored("QuickBooks module not available - using empty customer list", Colors.YELLOW)
            return pd.DataFrame(columns=['email'])
        except Exception as e:
            print_colored(f"Error loading QuickBooks customers: {e}", Colors.RED)
            return pd.DataFrame(columns=['email'])

    def load_seo_data_from_csv(self, file_path: str) -> pd.DataFrame:
        """Load SEO keyword data from CSV file"""
        try:
            seo_df = pd.read_csv(file_path)
            
            # Rename columns to match expected format if needed
            if 'Keyphrase' in seo_df.columns and 'keyphrase' not in seo_df.columns:
                seo_df = seo_df.rename(columns={'Keyphrase': 'keyphrase'})
            
            if 'Current Position' in seo_df.columns:
                seo_df = seo_df.rename(columns={'Current Position': 'current_position'})
            
            # Convert position to numeric
            seo_df['current_position'] = pd.to_numeric(seo_df['current_position'], errors='coerce')
            seo_df['current_position'] = seo_df['current_position'].fillna(100)
            
            # Add product category based on keyphrase
            seo_df['product_category'] = seo_df['keyphrase'].apply(self.extract_product_category_from_keyword)
            
            return seo_df
            
        except Exception as e:
            print_colored(f"Error loading SEO data from CSV: {e}", Colors.RED)
            return pd.DataFrame()

    def create_mock_seo_data(self) -> pd.DataFrame:
        """Create mock SEO data based on products from lead analyzer"""
        mock_keywords = [
            ('custom bags singapore', 3),
            ('business cards printing', 1),
            ('corporate gifts singapore', 2),
            ('lanyards custom printing', 5),
            ('custom badges singapore', 4),
            ('promotional items', 8),
            ('custom stickers printing', 6),
            ('corporate merchandise', 7),
            ('custom notebooks singapore', 9),
            ('branded pens singapore', 12),
            ('custom mugs printing', 10),
            ('promotional bags', 15),
            ('custom keychains singapore', 18),
            ('safety vests printing', 20),
            ('custom umbrellas', 25)
        ]
        
        seo_df = pd.DataFrame(mock_keywords, columns=['keyphrase', 'current_position'])
        seo_df['product_category'] = seo_df['keyphrase'].apply(self.extract_product_category_from_keyword)
        
        return seo_df

    def process_data(self):
        """Process and clean all data sources"""
        print_colored("Processing and cleaning data...", Colors.BLUE)

        # Create product-keyword mapping first
        self.create_product_keyword_mapping()

        # Process leads data (adapted for leads_with_products.csv format)
        self.process_leads_data()

        # Process customer data
        self.process_customer_data()

        # Process and combine PPC data
        self.process_ppc_data()

    def process_leads_data(self):
        """Process leads data from lead_analyzer output"""
        print_colored("Processing leads data from lead analyzer output...", Colors.BLUE)
        
        # The leads_with_products.csv has these columns:
        # email, original_classification, original_reason, total_tickets_analyzed, 
        # products_mentioned, ticket_subjects, analysis_period
        
        # Clean email addresses
        if 'email' in self.leads_df.columns:
            self.leads_df['email'] = self.leads_df['email'].astype(str).str.lower().str.strip()
        else:
            print_colored("Warning: No email column found in leads data", Colors.YELLOW)
            return

        # Create timestamp from analysis period or use current time
        if 'analysis_period' in self.leads_df.columns:
            # Extract end date from period like "March 2025 - May 2025"
            self.leads_df['first_inquiry_timestamp'] = self.leads_df['analysis_period'].apply(
                self.parse_analysis_period_to_date
            )
        else:
            self.leads_df['first_inquiry_timestamp'] = datetime.datetime.now()

        # Extract keywords from products_mentioned and ticket_subjects
        self.leads_df['extracted_keywords'] = self.leads_df.apply(
            lambda row: self.extract_keywords_from_lead_data(row), axis=1
        )

        # Initialize attribution columns
        self.leads_df['attributed_source'] = 'Unknown'
        self.leads_df['attribution_confidence'] = 0
        self.leads_df['attribution_detail'] = ''

        # Extract day of week and hour for temporal analysis
        self.leads_df['day_of_week'] = self.leads_df['first_inquiry_timestamp'].dt.day_name()
        self.leads_df['hour_of_day'] = self.leads_df['first_inquiry_timestamp'].dt.hour

        # Extract product information
        if 'products_mentioned' in self.leads_df.columns:
            self.leads_df['product'] = self.leads_df['products_mentioned'].fillna('')
        else:
            self.leads_df['product'] = ''

        # Extract subject information
        if 'ticket_subjects' in self.leads_df.columns:
            self.leads_df['subject'] = self.leads_df['ticket_subjects'].fillna('')
        else:
            self.leads_df['subject'] = ''

        print_colored("✓ Leads data processed", Colors.GREEN)

    def parse_analysis_period_to_date(self, period_str: str) -> datetime.datetime:
        """Parse analysis period string to datetime"""
        try:
            if isinstance(period_str, str) and ' - ' in period_str:
                # Extract end date from "March 2025 - May 2025"
                end_part = period_str.split(' - ')[1].strip()
                # Parse "May 2025" to datetime
                return datetime.datetime.strptime(end_part, "%B %Y")
            else:
                return datetime.datetime.now()
        except:
            return datetime.datetime.now()

    def extract_keywords_from_lead_data(self, row) -> List[str]:
        """Extract keywords from products_mentioned and ticket_subjects"""
        keywords = []
        
        # Extract from products_mentioned
        if 'products_mentioned' in row and pd.notna(row['products_mentioned']):
            products = str(row['products_mentioned']).split(';')
            for product in products:
                keywords.extend(self.extract_keywords_from_text(product.strip()))
        
        # Extract from ticket_subjects
        if 'ticket_subjects' in row and pd.notna(row['ticket_subjects']):
            subjects = str(row['ticket_subjects']).split(';')
            for subject in subjects:
                keywords.extend(self.extract_keywords_from_text(subject.strip()))
        
        return list(set(keywords))  # Remove duplicates

    def extract_keywords_from_text(self, text: str) -> List[str]:
        """Extract keywords from text string"""
        if not isinstance(text, str):
            return []

        # Convert to lowercase
        text = text.lower()

        # Extract words and phrases
        words = re.findall(r'\b\w+\b', text)

        # Return individual words and 2-3 word phrases
        result = words.copy()
        for i in range(len(words)-1):
            result.append(f"{words[i]} {words[i+1]}")
            if i < len(words)-2:
                result.append(f"{words[i]} {words[i+1]} {words[i+2]}")

        return result

    def process_customer_data(self):
        """Process and clean customer data"""
        if 'email' not in self.customers_df.columns:
            print_colored("Warning: No email column in customer data", Colors.YELLOW)
            self.customer_emails = set()
            return

        # Clean email addresses
        self.customers_df['email'] = self.customers_df['email'].astype(str).str.lower().str.strip()

        # Filter out invalid emails
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        valid_mask = self.customers_df['email'].str.match(email_pattern)

        # Create set of customer emails for faster lookup
        valid_emails = self.customers_df.loc[valid_mask, 'email'].dropna().unique()
        self.customer_emails = set(valid_emails)

        print_colored(f"✓ Customer data processed: {len(self.customer_emails)} unique emails", Colors.GREEN)

    def process_ppc_data(self):
        """Process and combine PPC campaign data"""
        if self.ppc_standard_df.empty and self.ppc_dynamic_df.empty:
            print_colored("No PPC data to process", Colors.YELLOW)
            self.combined_ppc_df = pd.DataFrame()
            return

        # Create mock PPC data if needed
        if self.ppc_standard_df.empty:
            self.ppc_standard_df = self.create_mock_ppc_data('Standard')
        if self.ppc_dynamic_df.empty:
            self.ppc_dynamic_df = self.create_mock_ppc_data('Dynamic')

        # Combine PPC data
        common_columns = ['date', 'keyword', 'clicks', 'impressions', 'campaign_type']
        
        frames_to_concat = []
        if not self.ppc_standard_df.empty:
            frames_to_concat.append(self.ppc_standard_df[common_columns])
        if not self.ppc_dynamic_df.empty:
            frames_to_concat.append(self.ppc_dynamic_df[common_columns])

        if frames_to_concat:
            self.combined_ppc_df = pd.concat(frames_to_concat).reset_index(drop=True)
            self.combined_ppc_df['day_of_week'] = self.combined_ppc_df['date'].dt.day_name()
            self.combined_ppc_df['hour_of_day'] = 0
        else:
            self.combined_ppc_df = pd.DataFrame()

        print_colored("✓ PPC data processed", Colors.GREEN)

    def create_mock_ppc_data(self, campaign_type: str) -> pd.DataFrame:
        """Create mock PPC data for testing"""
        mock_data = []
        base_date = datetime.datetime.now() - datetime.timedelta(days=30)
        
        keywords = [
            'custom bags', 'business cards', 'corporate gifts', 'lanyards',
            'custom badges', 'promotional items', 'custom stickers'
        ]
        
        for i in range(20):
            date = base_date + datetime.timedelta(days=i)
            keyword = keywords[i % len(keywords)]
            clicks = np.random.randint(1, 10)
            impressions = np.random.randint(10, 100)
            
            mock_data.append({
                'date': date,
                'keyword': keyword,
                'clicks': clicks,
                'impressions': impressions,
                'campaign_type': campaign_type
            })
        
        return pd.DataFrame(mock_data)

    def create_product_keyword_mapping(self):
        """Create mapping between products and keywords"""
        self.product_keyword_map = {
            'bags': ['bag', 'bags', 'tote', 'canvas', 'drawstring', 'mesh', 'paper bag'],
            'cards': ['card', 'cards', 'business card', 'name card', 'visiting card'],
            'badges': ['badge', 'badges', 'pin badge', 'enamel badge', 'lapel pin'],
            'lanyards': ['lanyard', 'lanyards', 'neck strap', 'id holder'],
            'stickers': ['sticker', 'stickers', 'vinyl', 'decal', 'label'],
            'notebooks': ['notebook', 'notepad', 'journal', 'writing pad'],
            'pens': ['pen', 'pens', 'ballpoint', 'writing instrument'],
            'mugs': ['mug', 'mugs', 'cup', 'ceramic', 'coffee mug'],
            'keychains': ['keychain', 'key ring', 'key holder', 'key tag'],
            'safety_items': ['vest', 'safety', 'hi-vis', 'high visibility'],
            'umbrellas': ['umbrella', 'parasol', 'rain protection'],
            'promotional': ['promotional', 'corporate', 'branded', 'custom']
        }

    def extract_product_category_from_keyword(self, keyword: str) -> str:
        """Extract product category from keyword text"""
        if not isinstance(keyword, str):
            return 'other'

        keyword = keyword.lower()

        # Check each product category's keywords for matches
        for category, terms in self.product_keyword_map.items():
            for term in terms:
                if term in keyword:
                    return category

        return 'other'

    def run_attribution(self) -> pd.DataFrame:
        """Run the full attribution process"""
        print_colored("Starting attribution analysis...", Colors.BOLD + Colors.BLUE)

        # Step 1: Identify direct traffic (returning customers)
        self.identify_direct_traffic()

        # Step 2: Identify SEO traffic
        self.identify_seo_traffic()

        # Step 3: Identify potential referrals
        self.identify_referrals()

        # Step 4: Identify PPC traffic
        self.identify_ppc_traffic()

        # Step 5: Calculate confidence scores and finalize attribution
        self.finalize_attribution()

        print_colored("✓ Attribution analysis completed", Colors.GREEN)
        return self.leads_df

    def identify_direct_traffic(self):
        """Identify direct traffic from returning customers"""
        print_colored("Identifying direct traffic...", Colors.BLUE)

        # Check if each lead email is in the customer list
        direct_mask = self.leads_df['email'].isin(self.customer_emails)

        # Mark direct traffic
        self.leads_df.loc[direct_mask, 'attributed_source'] = 'Direct'
        self.leads_df.loc[direct_mask, 'attribution_confidence'] = 100
        self.leads_df.loc[direct_mask, 'attribution_detail'] = 'Returning customer'

        direct_count = direct_mask.sum()
        print_colored(f"✓ Identified {direct_count} leads as direct traffic ({direct_count/len(self.leads_df)*100:.1f}%)", Colors.GREEN)

    def identify_seo_traffic(self):
        """Identify traffic from SEO"""
        print_colored("Identifying SEO traffic...", Colors.BLUE)

        if self.seo_keywords_df.empty:
            print_colored("No SEO data available - skipping SEO attribution", Colors.YELLOW)
            return

        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        seo_count = 0

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Match lead keywords with SEO keywords
            keyword_match_score = 0
            matched_keywords = []
            matched_positions = []

            for _, seo_kw in self.seo_keywords_df.iterrows():
                seo_keyword = seo_kw['keyphrase'].lower()
                seo_keyword_terms = self.extract_keywords_from_text(seo_keyword)

                for lead_kw in lead_keywords:
                    for seo_kw_term in seo_keyword_terms:
                        if FUZZY_AVAILABLE:
                            similarity = fuzz.token_sort_ratio(lead_kw, seo_kw_term)
                        else:
                            similarity = 100 if lead_kw == seo_kw_term else 0
                        
                        if similarity > 60:
                            # Higher score for better rankings
                            position_bonus = max(0, 10 - seo_kw['current_position']) * 3
                            adjusted_score = similarity + position_bonus
                            keyword_match_score = max(keyword_match_score, adjusted_score)
                            matched_keywords.append((lead_kw, seo_kw_term, similarity))
                            matched_positions.append(seo_kw['current_position'])

            # Calculate overall SEO confidence score
            if keyword_match_score > 0:
                position_score = 0
                if matched_positions:
                    avg_position = sum(matched_positions) / len(matched_positions)
                    if avg_position <= 1:
                        position_score = 100
                    elif avg_position <= 3:
                        position_score = 90
                    elif avg_position <= 5:
                        position_score = 80
                    elif avg_position <= 10:
                        position_score = 70
                    else:
                        position_score = 60

                confidence_score = (0.7 * keyword_match_score) + (0.3 * position_score)
                confidence_score = min(100, confidence_score)

                if confidence_score >= self.confidence_thresholds['low']:
                    self.leads_df.loc[idx, 'attributed_source'] = 'SEO'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score

                    matched_kw_str = '; '.join([f"{l}-{s}" for l, s, p in matched_keywords[:3]])
                    avg_pos = sum(matched_positions) / len(matched_positions) if matched_positions else 0
                    detail = f"Keyword matches: {matched_kw_str}, Avg position: {avg_pos:.1f}"
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    seo_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {seo_count} leads as SEO traffic ({seo_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)

    def identify_ppc_traffic(self):
        """Identify traffic from PPC campaigns"""
        print_colored("Identifying PPC traffic...", Colors.BLUE)

        if self.combined_ppc_df.empty:
            print_colored("No PPC data available - skipping PPC attribution", Colors.YELLOW)
            return

        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        ppc_count = 0

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_time = lead['first_inquiry_timestamp']
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Define time window for attribution
            time_window_start = lead_time - datetime.timedelta(hours=self.attribution_window_hours)
            
            # Find PPC clicks within time window
            ppc_clicks_in_window = self.combined_ppc_df[
                (self.combined_ppc_df['date'] >= time_window_start.date()) & 
                (self.combined_ppc_df['date'] <= lead_time.date()) & 
                (self.combined_ppc_df['clicks'] > 0)
            ]

            if ppc_clicks_in_window.empty:
                continue

            # Match lead keywords with PPC keywords
            keyword_match_score = 0
            matched_keywords = []

            for _, ppc_click in ppc_clicks_in_window.iterrows():
                ppc_keyword = ppc_click['keyword'].lower()
                ppc_keyword_terms = self.extract_keywords_from_text(ppc_keyword)

                for lead_kw in lead_keywords:
                    for ppc_kw in ppc_keyword_terms:
                        if FUZZY_AVAILABLE:
                            similarity = fuzz.token_sort_ratio(lead_kw, ppc_kw)
                        else:
                            similarity = 100 if lead_kw == ppc_kw else 0
                        
                        if similarity > 60:
                            keyword_match_score = max(keyword_match_score, similarity)
                            matched_keywords.append((lead_kw, ppc_kw, similarity))

            # Calculate time proximity score
            time_proximity_score = 0
            if not ppc_clicks_in_window.empty:
                lead_date = pd.Timestamp(lead_time.date())
                ppc_clicks_in_window = ppc_clicks_in_window.copy()
                ppc_clicks_in_window['date_diff'] = (lead_date - pd.to_datetime(ppc_clicks_in_window['date'])).dt.days
                
                if not ppc_clicks_in_window.empty:
                    min_days_diff = ppc_clicks_in_window['date_diff'].min()
                    
                    if min_days_diff == 0:
                        time_proximity_score = 100
                    elif min_days_diff == 1:
                        time_proximity_score = 90
                    else:
                        time_proximity_score = max(0, 100 - (min_days_diff * 15))

            # Calculate overall PPC confidence score
            if keyword_match_score > 0 and time_proximity_score > 0:
                confidence_score = (0.6 * keyword_match_score) + (0.4 * time_proximity_score)

                if confidence_score >= self.confidence_thresholds['low']:
                    self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score

                    matched_kw_str = '; '.join([f"{l}-{p}" for l, p, s in matched_keywords[:3]])
                    detail = f"Keyword matches: {matched_kw_str}, Time proximity: {time_proximity_score:.1f}%"
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    ppc_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {ppc_count} leads as PPC traffic ({ppc_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)

    def identify_referrals(self):
        """Identify potential referral traffic"""
        print_colored("Identifying potential referrals...", Colors.BLUE)

        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'

        # Extract email domains
        self.leads_df['email_domain'] = self.leads_df['email'].apply(
            lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else ''
        )

        # Count emails per domain
        domain_counts = self.leads_df['email_domain'].value_counts()
        multiple_lead_domains = domain_counts[domain_counts > 1].index.tolist()

        # Look for temporal clusters
        if 'first_inquiry_timestamp' in self.leads_df.columns:
            self.leads_df['inquiry_date'] = self.leads_df['first_inquiry_timestamp'].dt.date
            date_counts = self.leads_df['inquiry_date'].value_counts()
            busy_dates = date_counts[date_counts > 2].index.tolist()
        else:
            busy_dates = []

        referral_count = 0

        # Identify potential referrals
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            referral_score = 0
            referral_evidence = []

            # Check domain pattern
            if lead['email_domain'] in multiple_lead_domains:
                domain_count = domain_counts[lead['email_domain']]
                domain_score = min(60, domain_count * 15)
                referral_score += domain_score
                referral_evidence.append(f"Domain pattern: {domain_count} leads from {lead['email_domain']}")

            # Check temporal clusters
            if hasattr(lead['first_inquiry_timestamp'], 'date') and lead['first_inquiry_timestamp'].date() in busy_dates:
                inquiry_time = lead['first_inquiry_timestamp']
                time_window_start = inquiry_time - datetime.timedelta(hours=3)
                time_window_end = inquiry_time + datetime.timedelta(hours=3)

                time_window_inquiries = self.leads_df[
                    (self.leads_df['first_inquiry_timestamp'] >= time_window_start) &
                    (self.leads_df['first_inquiry_timestamp'] <= time_window_end) &
                    (self.leads_df.index != idx)
                ]

                time_cluster_count = len(time_window_inquiries)
                if time_cluster_count > 0:
                    time_score = min(40, time_cluster_count * 10)
                    referral_score += time_score
                    referral_evidence.append(f"Time cluster: {time_cluster_count} leads within 3 hours")

            # Calculate overall referral confidence score
            confidence_score = min(100, referral_score)

            if confidence_score >= self.confidence_thresholds['low']:
                self.leads_df.loc[idx, 'attributed_source'] = 'Referral'
                self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score
                self.leads_df.loc[idx, 'attribution_detail'] = '; '.join(referral_evidence)

                referral_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {referral_count} leads as Referral traffic ({referral_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)

    def finalize_attribution(self):
        """Finalize attribution and set confidence levels"""
        # Categorize confidence levels
        self.leads_df['confidence_level'] = self.leads_df['attribution_confidence'].apply(
            lambda score: 'High' if score >= self.confidence_thresholds['high'] else 
                         ('Medium' if score >= self.confidence_thresholds['medium'] else 
                          ('Low' if score >= self.confidence_thresholds['low'] else 'Unknown'))
        )

        # Count final attribution by source
        attribution_counts = self.leads_df['attributed_source'].value_counts()

        print_colored("\n=== Final Attribution Summary ===", Colors.BOLD + Colors.BLUE)
        for source, count in attribution_counts.items():
            print_colored(f"  {source}: {count} leads ({count/len(self.leads_df)*100:.1f}%)", Colors.GREEN)

        # Count by confidence level
        confidence_counts = self.leads_df['confidence_level'].value_counts()

        print_colored("\nAttribution Confidence Summary:", Colors.BLUE)
        for level, count in confidence_counts.items():
            print_colored(f"  {level}: {count} leads ({count/len(self.leads_df)*100:.1f}%)", Colors.GREEN)

    def save_results(self, output_path: str = "./output/leads_with_attribution.csv"):
        """Save attribution results to CSV"""
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to CSV
            self.leads_df.to_csv(output_path, index=False)
            print_colored(f"✓ Attribution results saved to {output_path}", Colors.GREEN)
            
            return True
        except Exception as e:
            print_colored(f"Error saving results: {e}", Colors.RED)
            return False

def analyze_traffic_attribution(leads_path="./output/leads_with_products.csv",
                              seo_csv_path=None,
                              ppc_standard_path=None,
                              ppc_dynamic_path=None,
                              output_path="./output/leads_with_attribution.csv"):
    """Main function to run traffic attribution analysis"""
    try:
        print_colored("=== Traffic Attribution Analysis ===", Colors.BOLD + Colors.BLUE)
        
        # Initialize analyzer
        analyzer = LeadAttributionAnalyzer()
        
        # Load data
        analyzer.load_data(
            leads_path=leads_path,
            seo_csv_path=seo_csv_path,
            ppc_standard_path=ppc_standard_path,
            ppc_dynamic_path=ppc_dynamic_path
        )
        
        # Run attribution
        attributed_leads = analyzer.run_attribution()
        
        # Save results
        success = analyzer.save_results(output_path)
        
        if success:
            print_colored(f"\n✓ Traffic attribution analysis completed successfully!", Colors.GREEN)
            print_colored(f"Results saved to: {output_path}", Colors.BLUE)
            return len(attributed_leads)
        else:
            print_colored("Traffic attribution analysis completed with errors", Colors.YELLOW)
            return 0
            
    except Exception as e:
        print_colored(f"Error in traffic attribution analysis: {e}", Colors.RED)
        return 0

if __name__ == "__main__":
    analyze_traffic_attribution()
