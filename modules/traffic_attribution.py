
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

# Import our data loader
from .traffic_data_loader import TrafficDataLoader

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
    def __init__(self, use_gsc_data=False, gsc_client=None, compare_methods=False):
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
        
        # Google Search Console integration parameters
        self.use_gsc_data = use_gsc_data
        self.gsc_client = gsc_client
        self.gsc_click_data = None
        
        # Comparison mode for testing different attribution methods
        self.compare_methods = compare_methods

    def load_data(self, 
                 leads_path="./output/leads_with_products.csv", 
                 seo_csv_path=None, 
                 ppc_standard_path=None, 
                 ppc_dynamic_path=None):
        """Load all data sources using TrafficDataLoader"""
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

        # Initialize traffic data loader
        self.traffic_loader = TrafficDataLoader()
        
        # Load traffic data using the dedicated loader
        traffic_data = self.traffic_loader.load_all_data(
            seo_path=seo_csv_path,
            ppc_standard_path=ppc_standard_path,
            ppc_dynamic_path=ppc_dynamic_path
        )
        
        # Assign loaded data to class attributes
        self.seo_keywords_df = traffic_data['seo_data']
        self.ppc_standard_df = traffic_data['ppc_standard_data']
        self.ppc_dynamic_df = traffic_data['ppc_dynamic_data']
        
        # Create fallback data if nothing was loaded
        if self.seo_keywords_df is None:
            print_colored("Creating mock SEO data for analysis", Colors.YELLOW)
            self.seo_keywords_df = self.create_mock_seo_data()
            
        if self.ppc_standard_df is None:
            self.ppc_standard_df = pd.DataFrame()
            
        if self.ppc_dynamic_df is None:
            self.ppc_dynamic_df = pd.DataFrame()

        # Show traffic data summary
        summary = traffic_data['summary']
        print_colored(f"✓ Traffic data summary: {summary['seo_keywords']} SEO, {summary['ppc_standard_keywords']} PPC standard, {summary['ppc_dynamic_targets']} PPC dynamic", Colors.BLUE)

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
        
        # Prepare external data sources (GSC, etc.)
        self.prepare_for_external_data_sources()

    def process_leads_data(self):
        """Process leads data from lead_analyzer output"""
        print_colored("Processing leads data from lead analyzer output...", Colors.BLUE)
        
        # The leads_with_products.csv has these columns:
        # email, original_classification, original_reason, total_tickets_analyzed, 
        # products_mentioned, ticket_subjects, analysis_period, first_ticket_date, 
        # last_ticket_date, most_recent_update
        
        # Clean email addresses
        if 'email' in self.leads_df.columns:
            self.leads_df['email'] = self.leads_df['email'].astype(str).str.lower().str.strip()
        else:
            print_colored("Warning: No email column found in leads data", Colors.YELLOW)
            return

        # Parse actual timestamp data from lead_analyzer
        print_colored("Processing timestamp data from tickets...", Colors.BLUE)
        
        # Convert timestamp columns to datetime objects
        if 'first_ticket_date' in self.leads_df.columns:
            # Parse first_ticket_date as primary timestamp
            self.leads_df['first_inquiry_timestamp'] = pd.to_datetime(
                self.leads_df['first_ticket_date'], 
                errors='coerce'
            )
            print_colored(f"✓ Parsed first_ticket_date for {self.leads_df['first_inquiry_timestamp'].notna().sum()} leads", Colors.GREEN)
        else:
            print_colored("Warning: No first_ticket_date column found - using current time", Colors.YELLOW)
            self.leads_df['first_inquiry_timestamp'] = pd.Timestamp.now()

        # Parse additional timestamp columns for analysis
        if 'last_ticket_date' in self.leads_df.columns:
            self.leads_df['last_ticket_timestamp'] = pd.to_datetime(
                self.leads_df['last_ticket_date'], 
                errors='coerce'
            )
        
        if 'most_recent_update' in self.leads_df.columns:
            self.leads_df['most_recent_update_timestamp'] = pd.to_datetime(
                self.leads_df['most_recent_update'], 
                errors='coerce'
            )

        # Handle leads with missing timestamps
        missing_timestamps = self.leads_df['first_inquiry_timestamp'].isna().sum()
        if missing_timestamps > 0:
            print_colored(f"Warning: {missing_timestamps} leads have missing first_ticket_date", Colors.YELLOW)
            # Use analysis_period as fallback for missing timestamps
            if 'analysis_period' in self.leads_df.columns:
                fallback_mask = self.leads_df['first_inquiry_timestamp'].isna()
                self.leads_df.loc[fallback_mask, 'first_inquiry_timestamp'] = self.leads_df.loc[fallback_mask, 'analysis_period'].apply(
                    self.parse_analysis_period_to_date
                )

        # Extract keywords from products_mentioned and ticket_subjects
        self.leads_df['extracted_keywords'] = self.leads_df.apply(
            lambda row: self.extract_keywords_from_lead_data(row), axis=1
        )

        # Initialize attribution columns
        self.leads_df['attributed_source'] = 'Unknown'
        self.leads_df['attribution_confidence'] = 0
        self.leads_df['attribution_detail'] = ''
        self.leads_df['data_source'] = 'unknown'
        
        # Initialize comparison columns if in comparison mode
        if self.compare_methods:
            self.leads_df['csv_attribution'] = 'Unknown'
            self.leads_df['csv_confidence'] = 0
            self.leads_df['api_attribution'] = 'Unknown'
            self.leads_df['api_confidence'] = 0

        # Extract day of week and hour for temporal analysis using real timestamps
        valid_timestamp_mask = self.leads_df['first_inquiry_timestamp'].notna()
        if valid_timestamp_mask.sum() > 0:
            self.leads_df.loc[valid_timestamp_mask, 'day_of_week'] = self.leads_df.loc[valid_timestamp_mask, 'first_inquiry_timestamp'].dt.day_name()
            self.leads_df.loc[valid_timestamp_mask, 'hour_of_day'] = self.leads_df.loc[valid_timestamp_mask, 'first_inquiry_timestamp'].dt.hour
        
        # Fill missing temporal data
        self.leads_df['day_of_week'] = self.leads_df['day_of_week'].fillna('Unknown')
        self.leads_df['hour_of_day'] = self.leads_df['hour_of_day'].fillna(0)

        # Extract product information directly
        if 'products_mentioned' in self.leads_df.columns:
            self.leads_df['product'] = self.leads_df['products_mentioned'].fillna('')
        else:
            self.leads_df['product'] = ''

        # Extract subject information directly
        if 'ticket_subjects' in self.leads_df.columns:
            self.leads_df['subject'] = self.leads_df['ticket_subjects'].fillna('')
        else:
            self.leads_df['subject'] = ''

        # Calculate ticket activity span for additional insights
        if 'last_ticket_timestamp' in self.leads_df.columns:
            both_timestamps_mask = (
                self.leads_df['first_inquiry_timestamp'].notna() & 
                self.leads_df['last_ticket_timestamp'].notna()
            )
            if both_timestamps_mask.sum() > 0:
                self.leads_df.loc[both_timestamps_mask, 'ticket_span_days'] = (
                    self.leads_df.loc[both_timestamps_mask, 'last_ticket_timestamp'] - 
                    self.leads_df.loc[both_timestamps_mask, 'first_inquiry_timestamp']
                ).dt.days

        print_colored("✓ Leads data processed with real timestamp data", Colors.GREEN)
        
        # Debug information
        timestamp_stats = self.leads_df['first_inquiry_timestamp'].describe()
        print_colored(f"Timestamp range: {timestamp_stats['min']} to {timestamp_stats['max']}", Colors.BLUE)

    def prepare_for_external_data_sources(self):
        """Prepare analyzer for external data sources like Google Search Console"""
        print_colored("Preparing for external data source integration...", Colors.BLUE)
        
        # Initialize Google Search Console data if enabled
        if self.use_gsc_data and self.gsc_client:
            print_colored("GSC integration enabled - loading click data", Colors.BLUE)
            try:
                self.gsc_click_data = self.get_gsc_click_data()
                print_colored(f"✓ Loaded {len(self.gsc_click_data)} GSC click records", Colors.GREEN)
            except Exception as e:
                print_colored(f"Warning: Could not load GSC data: {e}", Colors.YELLOW)
                self.gsc_click_data = pd.DataFrame()
        else:
            print_colored("GSC integration disabled - using CSV fallback", Colors.BLUE)
            self.gsc_click_data = pd.DataFrame()

    def get_gsc_click_data(self) -> pd.DataFrame:
        """
        Get click data from Google Search Console API
        TODO: Implement GSC API integration in Phase 2
        Returns empty DataFrame as placeholder
        """
        # TODO: Phase 2 - Implement Google Search Console API integration
        # This method will:
        # 1. Use self.gsc_client to authenticate with GSC API
        # 2. Query search analytics for clicks, impressions, positions
        # 3. Filter by date range matching lead timestamps
        # 4. Return DataFrame with columns: ['date', 'query', 'clicks', 'impressions', 'position', 'page']
        
        print_colored("GSC API integration not yet implemented - returning empty DataFrame", Colors.YELLOW)
        return pd.DataFrame(columns=['date', 'query', 'clicks', 'impressions', 'position', 'page'])

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
        self.leads_df.loc[direct_mask, 'data_source'] = 'customer_db'

        direct_count = direct_mask.sum()
        print_colored(f"✓ Identified {direct_count} leads as direct traffic ({direct_count/len(self.leads_df)*100:.1f}%)", Colors.GREEN)

    def identify_seo_traffic(self):
        """Identify traffic from SEO using GSC data first, then CSV fallback"""
        print_colored("Identifying SEO traffic...", Colors.BLUE)

        # TODO: Phase 2 - Implement GSC-first attribution logic
        # Check for GSC data first
        if self.use_gsc_data and not self.gsc_click_data.empty:
            print_colored("Using Google Search Console data for SEO attribution", Colors.BLUE)
            seo_count = self._identify_seo_from_gsc()
            self._update_data_source_for_seo('gsc_api')
        elif not self.seo_keywords_df.empty:
            print_colored("Using CSV data for SEO attribution", Colors.BLUE)
            seo_count = self._identify_seo_from_csv()
            self._update_data_source_for_seo('seo_csv')
        else:
            print_colored("No SEO data available - skipping SEO attribution", Colors.YELLOW)
            return

        # TODO: Phase 2 - If comparison mode is enabled, run both methods
        if self.compare_methods and not self.seo_keywords_df.empty and not self.gsc_click_data.empty:
            print_colored("Comparison mode: Running both CSV and GSC attribution methods", Colors.BLUE)
            # Store current results as CSV method
            csv_mask = self.leads_df['attributed_source'] == 'SEO'
            self.leads_df.loc[csv_mask, 'csv_attribution'] = 'SEO'
            self.leads_df.loc[csv_mask, 'csv_confidence'] = self.leads_df.loc[csv_mask, 'attribution_confidence']
            
            # Reset and run GSC method
            self.leads_df.loc[csv_mask, 'attributed_source'] = 'Unknown'
            self.leads_df.loc[csv_mask, 'attribution_confidence'] = 0
            gsc_count = self._identify_seo_from_gsc()
            
            # Store GSC results
            gsc_mask = self.leads_df['attributed_source'] == 'SEO'
            self.leads_df.loc[gsc_mask, 'api_attribution'] = 'SEO'
            self.leads_df.loc[gsc_mask, 'api_confidence'] = self.leads_df.loc[gsc_mask, 'attribution_confidence']

    def _identify_seo_from_gsc(self) -> int:
        """
        Identify SEO traffic using Google Search Console data
        TODO: Phase 2 - Implement GSC-based attribution
        """
        # TODO: Phase 2 - Implement GSC attribution logic
        # This method will:
        # 1. Match lead timestamps with GSC click data
        # 2. Use actual click/impression data for confidence scoring
        # 3. Match lead keywords with GSC query data
        # 4. Use real position data from GSC instead of CSV estimates
        
        print_colored("GSC-based SEO attribution not yet implemented", Colors.YELLOW)
        return 0

    def _identify_seo_from_csv(self) -> int:
        """Identify SEO traffic using CSV keyword data (current implementation)"""
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
        
        return seo_count

    def _update_data_source_for_seo(self, source_type: str):
        """Update data_source field for SEO attributed leads"""
        seo_mask = self.leads_df['attributed_source'] == 'SEO'
        self.leads_df.loc[seo_mask, 'data_source'] = source_type
        
        # Update attribution detail to include data source
        current_details = self.leads_df.loc[seo_mask, 'attribution_detail']
        self.leads_df.loc[seo_mask, 'attribution_detail'] = current_details + f" (source: {source_type})"

    def identify_ppc_traffic(self):
        """Identify traffic from PPC campaigns using real timestamps"""
        print_colored("Identifying PPC traffic with real timestamp matching...", Colors.BLUE)

        if self.combined_ppc_df.empty:
            print_colored("No PPC data available - skipping PPC attribution", Colors.YELLOW)
            return

        # Only consider leads not already attributed and with valid timestamps
        unattributed_mask = (
            (self.leads_df['attributed_source'] == 'Unknown') & 
            (self.leads_df['first_inquiry_timestamp'].notna())
        )
        ppc_count = 0

        if unattributed_mask.sum() == 0:
            print_colored("No unattributed leads with valid timestamps for PPC analysis", Colors.YELLOW)
            return

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_time = lead['first_inquiry_timestamp']
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Ensure lead_time is timezone-aware for comparison
            if lead_time.tz is None:
                lead_time = lead_time.tz_localize('UTC')

            # Define time window for attribution using real timestamps
            time_window_start = lead_time - pd.Timedelta(hours=self.attribution_window_hours)
            time_window_end = lead_time + pd.Timedelta(hours=2)  # Small buffer after lead time
            
            # Find PPC clicks within time window using proper datetime comparison
            ppc_clicks_in_window = self.combined_ppc_df[
                (pd.to_datetime(self.combined_ppc_df['date']) >= time_window_start.normalize()) & 
                (pd.to_datetime(self.combined_ppc_df['date']) <= time_window_end.normalize()) & 
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

            # Calculate time proximity score using real timestamps
            time_proximity_score = 0
            if not ppc_clicks_in_window.empty:
                # Calculate actual time differences in hours
                ppc_clicks_in_window = ppc_clicks_in_window.copy()
                ppc_dates = pd.to_datetime(ppc_clicks_in_window['date'])
                
                # Calculate time differences in hours
                time_diffs = []
                for ppc_date in ppc_dates:
                    if ppc_date.tz is None:
                        ppc_date = ppc_date.tz_localize('UTC')
                    
                    time_diff_hours = abs((lead_time - ppc_date).total_seconds() / 3600)
                    time_diffs.append(time_diff_hours)
                
                if time_diffs:
                    min_hours_diff = min(time_diffs)
                    
                    # Score based on hours rather than days for more precision
                    if min_hours_diff <= 1:
                        time_proximity_score = 100
                    elif min_hours_diff <= 6:
                        time_proximity_score = 95
                    elif min_hours_diff <= 12:
                        time_proximity_score = 85
                    elif min_hours_diff <= 24:
                        time_proximity_score = 75
                    elif min_hours_diff <= 48:
                        time_proximity_score = 60
                    else:
                        time_proximity_score = max(0, 50 - (min_hours_diff - 48) / 24 * 10)

            # Calculate overall PPC confidence score
            if keyword_match_score > 0 and time_proximity_score > 0:
                confidence_score = (0.6 * keyword_match_score) + (0.4 * time_proximity_score)

                if confidence_score >= self.confidence_thresholds['low']:
                    self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score
                    self.leads_df.loc[idx, 'data_source'] = 'ppc_csv'

                    matched_kw_str = '; '.join([f"{l}-{p}" for l, p, s in matched_keywords[:3]])
                    min_hours = min(time_diffs) if time_diffs else 0
                    detail = f"Keyword matches: {matched_kw_str}, Time gap: {min_hours:.1f}h, Proximity score: {time_proximity_score:.1f}% (source: ppc_csv)"
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    ppc_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {ppc_count} leads as PPC traffic ({ppc_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)

    def identify_referrals(self):
        """Identify potential referral traffic using real timestamps"""
        print_colored("Identifying potential referrals with real timestamp analysis...", Colors.BLUE)

        # Only consider leads not already attributed and with valid timestamps
        unattributed_mask = (
            (self.leads_df['attributed_source'] == 'Unknown') & 
            (self.leads_df['first_inquiry_timestamp'].notna())
        )

        if unattributed_mask.sum() == 0:
            print_colored("No unattributed leads with valid timestamps for referral analysis", Colors.YELLOW)
            return

        # Extract email domains
        self.leads_df['email_domain'] = self.leads_df['email'].apply(
            lambda x: x.split('@')[1] if isinstance(x, str) and '@' in x else ''
        )

        # Count emails per domain
        domain_counts = self.leads_df['email_domain'].value_counts()
        multiple_lead_domains = domain_counts[domain_counts > 1].index.tolist()

        # Look for temporal clusters using real timestamps
        valid_timestamp_leads = self.leads_df[self.leads_df['first_inquiry_timestamp'].notna()]
        if not valid_timestamp_leads.empty:
            # Group by date for temporal analysis
            valid_timestamp_leads = valid_timestamp_leads.copy()
            valid_timestamp_leads['inquiry_date'] = valid_timestamp_leads['first_inquiry_timestamp'].dt.date
            date_counts = valid_timestamp_leads['inquiry_date'].value_counts()
            busy_dates = date_counts[date_counts > 2].index.tolist()
            
            # Also look for hourly clusters (more precise)
            valid_timestamp_leads['inquiry_hour'] = valid_timestamp_leads['first_inquiry_timestamp'].dt.floor('H')
            hour_counts = valid_timestamp_leads['inquiry_hour'].value_counts()
            busy_hours = hour_counts[hour_counts > 1].index.tolist()
        else:
            busy_dates = []
            busy_hours = []

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

            # Check temporal clusters using real timestamps
            inquiry_time = lead['first_inquiry_timestamp']
            
            # Ensure timezone consistency
            if inquiry_time.tz is None:
                inquiry_time = inquiry_time.tz_localize('UTC')

            # Check daily clusters
            if inquiry_time.date() in busy_dates:
                daily_cluster_count = date_counts.get(inquiry_time.date(), 0) - 1  # Exclude current lead
                if daily_cluster_count > 0:
                    daily_score = min(30, daily_cluster_count * 8)
                    referral_score += daily_score
                    referral_evidence.append(f"Daily cluster: {daily_cluster_count} other leads on {inquiry_time.date()}")

            # Check hourly clusters (more precise referral detection)
            inquiry_hour = inquiry_time.floor('H')
            if inquiry_hour in busy_hours:
                # Define tighter time window for referral detection
                time_window_start = inquiry_time - pd.Timedelta(hours=2)
                time_window_end = inquiry_time + pd.Timedelta(hours=2)

                # Find leads in the same time window
                time_window_mask = (
                    (self.leads_df['first_inquiry_timestamp'] >= time_window_start) &
                    (self.leads_df['first_inquiry_timestamp'] <= time_window_end) &
                    (self.leads_df.index != idx) &
                    (self.leads_df['first_inquiry_timestamp'].notna())
                )
                
                time_window_inquiries = self.leads_df[time_window_mask]
                time_cluster_count = len(time_window_inquiries)
                
                if time_cluster_count > 0:
                    # Higher score for tighter time clusters
                    hourly_time_window_start = inquiry_time - pd.Timedelta(hours=1)
                    hourly_time_window_end = inquiry_time + pd.Timedelta(hours=1)
                    
                    hourly_cluster_mask = (
                        (time_window_inquiries['first_inquiry_timestamp'] >= hourly_time_window_start) &
                        (time_window_inquiries['first_inquiry_timestamp'] <= hourly_time_window_end)
                    )
                    
                    hourly_cluster_count = hourly_cluster_mask.sum()
                    
                    if hourly_cluster_count > 0:
                        time_score = min(50, hourly_cluster_count * 20)  # Higher score for tight clusters
                        referral_evidence.append(f"Tight time cluster: {hourly_cluster_count} leads within 1 hour")
                    else:
                        time_score = min(35, time_cluster_count * 12)
                        referral_evidence.append(f"Time cluster: {time_cluster_count} leads within 2 hours")
                    
                    referral_score += time_score

            # Additional referral indicators using ticket span data
            if 'ticket_span_days' in self.leads_df.columns and pd.notna(lead.get('ticket_span_days')):
                ticket_span = lead['ticket_span_days']
                # Short-lived inquiries might indicate referral traffic
                if ticket_span == 0:
                    referral_score += 10
                    referral_evidence.append("Single-day inquiry (referral indicator)")
                elif ticket_span <= 1:
                    referral_score += 5
                    referral_evidence.append("Short inquiry span (referral indicator)")

            # Calculate overall referral confidence score
            confidence_score = min(100, referral_score)

            if confidence_score >= self.confidence_thresholds['low']:
                self.leads_df.loc[idx, 'attributed_source'] = 'Referral'
                self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score
                self.leads_df.loc[idx, 'data_source'] = 'pattern'
                
                # Add timestamp info to referral details
                timestamp_info = f"Inquiry at {inquiry_time.strftime('%Y-%m-%d %H:%M')}"
                all_evidence = referral_evidence + [timestamp_info, "source: pattern"]
                self.leads_df.loc[idx, 'attribution_detail'] = '; '.join(all_evidence)

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
                              output_path="./output/leads_with_attribution.csv",
                              use_gsc_data=False,
                              gsc_client=None,
                              compare_methods=False):
    """Main function to run traffic attribution analysis"""
    try:
        print_colored("=== Traffic Attribution Analysis ===", Colors.BOLD + Colors.BLUE)
        
        # Initialize analyzer
        analyzer = LeadAttributionAnalyzer(
            use_gsc_data=use_gsc_data,
            gsc_client=gsc_client,
            compare_methods=compare_methods
        )
        
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
