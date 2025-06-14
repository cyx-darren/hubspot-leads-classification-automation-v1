
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
        self.customers_df = self.load_customers_from_quickbooks()
        
        # Check if we got customer data
        if len(self.customers_df) > 0:
            print_colored(f"✓ Loaded {len(self.customers_df)} customer records from QuickBooks", Colors.GREEN)
        else:
            print_colored("✓ Continuing without QuickBooks customer data", Colors.BLUE)

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
                print_colored("This means 'Direct' traffic attribution won't be available", Colors.YELLOW)
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
            print_colored("QuickBooks module not available - continuing without customer data", Colors.YELLOW)
            print_colored("This means 'Direct' traffic attribution won't be available", Colors.YELLOW)
            return pd.DataFrame(columns=['email'])
        except Exception as e:
            print_colored(f"Warning: QuickBooks authentication failed - continuing without customer data", Colors.YELLOW)
            print_colored(f"This means 'Direct' traffic attribution won't be available", Colors.YELLOW)
            print_colored(f"Error details: {e}", Colors.YELLOW)
            return pd.DataFrame(columns=['email'])

    def load_seo_data_from_csv(self, file_path: str) -> pd.DataFrame:
        """Load SEO keyword data from CSV file"""
        try:
            seo_df = pd.read_csv(file_path)
            
            # Your CSV has these exact columns: Keyphrase, Current Page, Current Position
            # Rename to lowercase for consistency
            seo_df = seo_df.rename(columns={
                'Keyphrase': 'keyphrase',
                'Current Page': 'current_page',
                'Current Position': 'current_position'
            })
            
            # Convert position to numeric (it's already integer but ensure)
            seo_df['current_position'] = pd.to_numeric(seo_df['current_position'], errors='coerce')
            seo_df['current_position'] = seo_df['current_position'].fillna(100)
            
            # Add product category based on keyphrase
            seo_df['product_category'] = seo_df['keyphrase'].apply(self.extract_product_category_from_keyword)
            
            print_colored(f"   ✓ Loaded {len(seo_df)} SEO keywords with positions", Colors.GREEN)
            
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
            self.leads_df['first_ticket_date'] = pd.to_datetime(
                self.leads_df['first_ticket_date'], 
                errors='coerce'
            )
            # Also create the first_inquiry_timestamp column for compatibility
            self.leads_df['first_inquiry_timestamp'] = self.leads_df['first_ticket_date']
            print_colored(f"✓ Parsed first_ticket_date for {self.leads_df['first_ticket_date'].notna().sum()} leads", Colors.GREEN)
        else:
            print_colored("Warning: No first_ticket_date column found - using current time", Colors.YELLOW)
            self.leads_df['first_ticket_date'] = pd.Timestamp.now()
            self.leads_df['first_inquiry_timestamp'] = self.leads_df['first_ticket_date']

        # Parse additional timestamp columns for analysis
        if 'last_ticket_date' in self.leads_df.columns:
            self.leads_df['last_ticket_date'] = pd.to_datetime(
                self.leads_df['last_ticket_date'], 
                errors='coerce'
            )
            self.leads_df['last_ticket_timestamp'] = self.leads_df['last_ticket_date']
        
        if 'most_recent_update' in self.leads_df.columns:
            self.leads_df['most_recent_update'] = pd.to_datetime(
                self.leads_df['most_recent_update'], 
                errors='coerce'
            )
            self.leads_df['most_recent_update_timestamp'] = self.leads_df['most_recent_update']

        # Handle leads with missing timestamps
        missing_timestamps = self.leads_df['first_ticket_date'].isna().sum()
        if missing_timestamps > 0:
            print_colored(f"Warning: {missing_timestamps} leads have missing first_ticket_date", Colors.YELLOW)
            # Use analysis_period as fallback for missing timestamps
            if 'analysis_period' in self.leads_df.columns:
                fallback_mask = self.leads_df['first_ticket_date'].isna()
                fallback_dates = self.leads_df.loc[fallback_mask, 'analysis_period'].apply(
                    self.parse_analysis_period_to_date
                )
                self.leads_df.loc[fallback_mask, 'first_ticket_date'] = pd.to_datetime(fallback_dates, errors='coerce')
                self.leads_df.loc[fallback_mask, 'first_inquiry_timestamp'] = self.leads_df.loc[fallback_mask, 'first_ticket_date']

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
        try:
            valid_timestamp_mask = self.leads_df['first_ticket_date'].notna()
            if valid_timestamp_mask.sum() > 0:
                self.leads_df.loc[valid_timestamp_mask, 'day_of_week'] = self.leads_df.loc[valid_timestamp_mask, 'first_ticket_date'].dt.day_name()
                self.leads_df.loc[valid_timestamp_mask, 'hour_of_day'] = self.leads_df.loc[valid_timestamp_mask, 'first_ticket_date'].dt.hour
            else:
                print_colored("Warning: No valid timestamps found for temporal analysis", Colors.YELLOW)
        except AttributeError as e:
            print_colored(f"Warning: Could not extract temporal data: {e}", Colors.YELLOW)
            # Initialize with default values
            self.leads_df['day_of_week'] = 'Unknown'
            self.leads_df['hour_of_day'] = 0
        
        # Fill missing temporal data
        if 'day_of_week' not in self.leads_df.columns:
            self.leads_df['day_of_week'] = 'Unknown'
        else:
            self.leads_df['day_of_week'] = self.leads_df['day_of_week'].fillna('Unknown')
        
        if 'hour_of_day' not in self.leads_df.columns:
            self.leads_df['hour_of_day'] = 0
        else:
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
        try:
            if 'last_ticket_date' in self.leads_df.columns and 'first_ticket_date' in self.leads_df.columns:
                both_timestamps_mask = (
                    self.leads_df['first_ticket_date'].notna() & 
                    self.leads_df['last_ticket_date'].notna()
                )
                if both_timestamps_mask.sum() > 0:
                    self.leads_df.loc[both_timestamps_mask, 'ticket_span_days'] = (
                        self.leads_df.loc[both_timestamps_mask, 'last_ticket_date'] - 
                        self.leads_df.loc[both_timestamps_mask, 'first_ticket_date']
                    ).dt.days
        except Exception as e:
            print_colored(f"Warning: Could not calculate ticket span: {e}", Colors.YELLOW)

        print_colored("✓ Leads data processed with real timestamp data", Colors.GREEN)
        
        # Debug information
        try:
            valid_timestamps = self.leads_df['first_ticket_date'].dropna()
            if len(valid_timestamps) > 0:
                timestamp_stats = valid_timestamps.describe()
                print_colored(f"Timestamp range: {timestamp_stats['min']} to {timestamp_stats['max']}", Colors.BLUE)
            else:
                print_colored("No valid timestamps available for statistics", Colors.YELLOW)
        except Exception as e:
            print_colored(f"Warning: Could not generate timestamp statistics: {e}", Colors.YELLOW)

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
        if self.customers_df.empty or 'email' not in self.customers_df.columns:
            print_colored("No customer data available - direct traffic attribution disabled", Colors.BLUE)
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

        frames_to_concat = []
        has_date_data = False

        # Process standard campaign data
        if not self.ppc_standard_df.empty:
            print_colored(f"   Processing PPC Standard data - columns: {list(self.ppc_standard_df.columns)}", Colors.BLUE)
            
            # Create a copy and process standard PPC data
            standard_df = self.ppc_standard_df.copy()
            
            # Map columns properly - use actual column names from the DataFrame
            if 'Keyword' in standard_df.columns:
                standard_df['keyword'] = standard_df['Keyword']
            else:
                print_colored("   Warning: 'Keyword' column not found in standard PPC data", Colors.YELLOW)
                continue
            
            if 'Clicks' in standard_df.columns:
                standard_df['clicks'] = pd.to_numeric(standard_df['Clicks'], errors='coerce').fillna(0)
            else:
                print_colored("   Warning: 'Clicks' column not found in standard PPC data", Colors.YELLOW)
                standard_df['clicks'] = 0
            
            if 'Impr.' in standard_df.columns:
                # Handle impressions with commas
                standard_df['impressions'] = pd.to_numeric(
                    standard_df['Impr.'].astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0)
            else:
                print_colored("   Warning: 'Impr.' column not found in standard PPC data", Colors.YELLOW)
                standard_df['impressions'] = 0
            
            # Add campaign type
            standard_df['campaign_type'] = 'Standard'
            
            # Check if date column exists
            if 'Date' in standard_df.columns or 'date' in standard_df.columns:
                date_col = 'Date' if 'Date' in standard_df.columns else 'date'
                standard_df['date'] = pd.to_datetime(standard_df[date_col], errors='coerce')
                has_date_data = True
                print_colored("   ✓ Date column found in standard PPC data", Colors.GREEN)
            else:
                print_colored("   Warning: PPC Standard data has no date column - time-based attribution disabled", Colors.YELLOW)
                standard_df['date'] = pd.NaT
                
            frames_to_concat.append(standard_df)

        # Process dynamic campaign data
        if not self.ppc_dynamic_df.empty:
            print_colored(f"   Processing PPC Dynamic data - columns: {list(self.ppc_dynamic_df.columns)}", Colors.BLUE)
            
            # Create a copy and process dynamic PPC data
            dynamic_df = self.ppc_dynamic_df.copy()
            
            # Map columns properly - dynamic uses 'Dynamic ad target' instead of 'Keyword'
            if 'Dynamic ad target' in dynamic_df.columns:
                dynamic_df['keyword'] = dynamic_df['Dynamic ad target']
            else:
                print_colored("   Warning: 'Dynamic ad target' column not found in dynamic PPC data", Colors.YELLOW)
                continue
            
            if 'Clicks' in dynamic_df.columns:
                dynamic_df['clicks'] = pd.to_numeric(dynamic_df['Clicks'], errors='coerce').fillna(0)
            else:
                print_colored("   Warning: 'Clicks' column not found in dynamic PPC data", Colors.YELLOW)
                dynamic_df['clicks'] = 0
            
            if 'Impr.' in dynamic_df.columns:
                # Handle impressions with commas
                dynamic_df['impressions'] = pd.to_numeric(
                    dynamic_df['Impr.'].astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0)
            else:
                print_colored("   Warning: 'Impr.' column not found in dynamic PPC data", Colors.YELLOW)
                dynamic_df['impressions'] = 0
            
            # Add campaign type
            dynamic_df['campaign_type'] = 'Dynamic'
            
            # Check if date column exists
            if 'Date' in dynamic_df.columns or 'date' in dynamic_df.columns:
                date_col = 'Date' if 'Date' in dynamic_df.columns else 'date'
                dynamic_df['date'] = pd.to_datetime(dynamic_df[date_col], errors='coerce')
                has_date_data = True
                print_colored("   ✓ Date column found in dynamic PPC data", Colors.GREEN)
            else:
                print_colored("   Warning: PPC Dynamic data has no date column - time-based attribution disabled", Colors.YELLOW)
                dynamic_df['date'] = pd.NaT
                
            frames_to_concat.append(dynamic_df)

        # Combine only the common columns we have
        common_columns = ['keyword', 'clicks', 'impressions', 'campaign_type']
        
        # Add date column if we have date data
        if has_date_data:
            common_columns.append('date')

        # Filter frames to only include those with all required columns
        valid_frames = []
        for df in frames_to_concat:
            if all(col in df.columns for col in common_columns):
                valid_frames.append(df[common_columns])
            else:
                missing_cols = [col for col in common_columns if col not in df.columns]
                print_colored(f"   Warning: Skipping frame missing columns: {missing_cols}", Colors.YELLOW)

        if valid_frames:
            self.combined_ppc_df = pd.concat(valid_frames, ignore_index=True)
            print_colored(f"   ✓ Combined PPC data: {len(self.combined_ppc_df)} total keywords", Colors.GREEN)
            
            # Clean keyword data
            self.combined_ppc_df['keyword'] = self.combined_ppc_df['keyword'].astype(str).str.lower().str.strip()
            
            # Add temporal columns for analysis
            if has_date_data and 'date' in self.combined_ppc_df.columns:
                try:
                    valid_dates = pd.notna(self.combined_ppc_df['date'])
                    if valid_dates.any():
                        self.combined_ppc_df.loc[valid_dates, 'day_of_week'] = self.combined_ppc_df.loc[valid_dates, 'date'].dt.day_name()
                        self.combined_ppc_df['hour_of_day'] = 0  # Default since we don't have hourly data
                        print_colored("   ✓ PPC data processed with date information", Colors.GREEN)
                    else:
                        self.combined_ppc_df['day_of_week'] = 'Unknown'
                        self.combined_ppc_df['hour_of_day'] = 0
                        print_colored("   Warning: No valid dates found in PPC data", Colors.YELLOW)
                except Exception as e:
                    print_colored(f"   Warning: Could not process PPC dates: {e}", Colors.YELLOW)
                    self.combined_ppc_df['day_of_week'] = 'Unknown'
                    self.combined_ppc_df['hour_of_day'] = 0
            else:
                self.combined_ppc_df['day_of_week'] = 'Unknown'
                self.combined_ppc_df['hour_of_day'] = 0
                print_colored("   ✓ PPC data processed without date information", Colors.YELLOW)
            
            # Filter out rows with no clicks
            before_filter = len(self.combined_ppc_df)
            self.combined_ppc_df = self.combined_ppc_df[self.combined_ppc_df['clicks'] > 0]
            after_filter = len(self.combined_ppc_df)
            
            if before_filter != after_filter:
                print_colored(f"   Filtered out {before_filter - after_filter} PPC entries with zero clicks", Colors.BLUE)
            
            print_colored(f"✓ Final PPC dataset: {len(self.combined_ppc_df)} entries with clicks", Colors.GREEN)
        else:
            print_colored("   Warning: No valid PPC data frames to combine", Colors.YELLOW)
            self.combined_ppc_df = pd.DataFrame()

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
        
        total_steps = 5
        
        # Step 1: Identify direct traffic (returning customers)
        self.display_progress_bar(1, total_steps, "Direct Traffic")
        self.identify_direct_traffic()

        # Step 2: Identify SEO traffic
        self.display_progress_bar(2, total_steps, "SEO Traffic")
        self.identify_seo_traffic()

        # Step 3: Identify potential referrals
        self.display_progress_bar(3, total_steps, "Referral Traffic")
        self.identify_referrals()

        # Step 4: Identify PPC traffic
        self.display_progress_bar(4, total_steps, "PPC Traffic")
        self.identify_ppc_traffic()

        # Step 5: Calculate confidence scores and finalize attribution
        self.display_progress_bar(5, total_steps, "Finalizing")
        self.finalize_attribution()

        print_colored("\n✓ Attribution analysis completed", Colors.GREEN)
        return self.leads_df

    def identify_direct_traffic(self):
        """Identify direct traffic from returning customers"""
        print_colored("Identifying direct traffic...", Colors.BLUE)

        if not self.customer_emails:
            print_colored("No customer data available - skipping direct traffic identification", Colors.YELLOW)
            return

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
                            # Higher score for better rankings (check column exists)
                            if 'current_position' in seo_kw and pd.notna(seo_kw['current_position']):
                                position_bonus = max(0, 10 - seo_kw['current_position']) * 3
                                adjusted_score = similarity + position_bonus
                                matched_positions.append(seo_kw['current_position'])
                            else:
                                adjusted_score = similarity
                                matched_positions.append(50)  # Default position if missing
                            
                            keyword_match_score = max(keyword_match_score, adjusted_score)
                            matched_keywords.append((lead_kw, seo_kw_term, similarity))

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

    def match_ppc_keywords_only(self, lead_keywords, ppc_keywords=None):
        """Match PPC keywords without time data - lower confidence"""
        best_match_score = 0
        matched_keywords = []
        
        for _, ppc_row in self.combined_ppc_df.iterrows():
            if ppc_row['clicks'] == 0:
                continue  # Skip keywords with no clicks
                
            ppc_keyword = str(ppc_row['keyword']).lower()
            
            for lead_kw in lead_keywords:
                if FUZZY_AVAILABLE:
                    similarity = fuzz.token_sort_ratio(lead_kw, ppc_keyword)
                else:
                    similarity = 100 if lead_kw == ppc_keyword else 0
                
                if similarity > 70:  # Higher threshold since no time validation
                    best_match_score = max(best_match_score, similarity)
                    matched_keywords.append((lead_kw, ppc_keyword, similarity))
        
        # Cap confidence at 60% since we can't verify timing
        confidence = min(60, best_match_score * 0.6)
        
        return confidence, matched_keywords

    def identify_ppc_traffic(self):
        """Identify traffic from PPC campaigns"""
        print_colored("Identifying PPC traffic...", Colors.BLUE)

        if self.combined_ppc_df.empty:
            print_colored("No PPC data available - skipping PPC attribution", Colors.YELLOW)
            return

        # Check if we have date data for time-based attribution
        has_valid_dates = False
        if 'date' in self.combined_ppc_df.columns:
            valid_dates = pd.to_datetime(self.combined_ppc_df['date'], errors='coerce').notna()
            has_valid_dates = valid_dates.any()

        if not has_valid_dates:
            print_colored("Note: PPC attribution using keyword matching only (no date data available)", Colors.YELLOW)
            print_colored("Warning: PPC attribution confidence will be limited due to missing timestamp data", Colors.YELLOW)
        else:
            print_colored("PPC attribution using keyword matching with time verification", Colors.BLUE)

        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        
        # If we have dates, also require valid timestamps on leads
        if has_valid_dates:
            unattributed_mask = unattributed_mask & self.leads_df['first_inquiry_timestamp'].notna()

        ppc_count = 0

        if unattributed_mask.sum() == 0:
            if has_valid_dates:
                print_colored("No unattributed leads with valid timestamps for PPC analysis", Colors.YELLOW)
            else:
                print_colored("No unattributed leads for PPC analysis", Colors.YELLOW)
            return

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Use different attribution methods based on data availability
            if has_valid_dates and pd.notna(lead.get('first_inquiry_timestamp')):
                # Time-based attribution (existing logic)
                ppc_data_to_check = self.combined_ppc_df.copy()
                time_proximity_score = 50
                time_diffs = []
                
                lead_time = lead['first_inquiry_timestamp']
                
                # Ensure lead_time is timezone-aware for comparison
                if lead_time.tz is None:
                    lead_time = lead_time.tz_localize('UTC')

                # Define time window for attribution
                time_window_start = lead_time - pd.Timedelta(hours=self.attribution_window_hours)
                time_window_end = lead_time + pd.Timedelta(hours=2)
                
                # Filter PPC data within time window
                ppc_dates = pd.to_datetime(ppc_data_to_check['date'], errors='coerce')
                valid_date_mask = ppc_dates.notna()
                
                if valid_date_mask.any():
                    time_window_mask = (
                        (ppc_dates >= time_window_start.normalize()) & 
                        (ppc_dates <= time_window_end.normalize()) &
                        valid_date_mask
                    )
                    
                    if time_window_mask.any():
                        ppc_data_to_check = ppc_data_to_check[time_window_mask]
                        
                        # Calculate time proximity score
                        filtered_dates = ppc_dates[time_window_mask]
                        for ppc_date in filtered_dates:
                            if pd.notna(ppc_date):
                                if ppc_date.tz is None:
                                    ppc_date = ppc_date.tz_localize('UTC')
                                
                                time_diff_hours = abs((lead_time - ppc_date).total_seconds() / 3600)
                                time_diffs.append(time_diff_hours)
                        
                        if time_diffs:
                            min_hours_diff = min(time_diffs)
                            
                            # Score based on hours
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
                    else:
                        # No PPC activity in time window
                        continue

                # Filter for campaigns with clicks
                ppc_data_to_check = ppc_data_to_check[ppc_data_to_check['clicks'] > 0]

                if ppc_data_to_check.empty:
                    continue

                # Match lead keywords with PPC keywords
                keyword_match_score = 0
                matched_keywords = []

                for _, ppc_row in ppc_data_to_check.iterrows():
                    ppc_keyword = str(ppc_row['keyword']).lower()
                    ppc_keyword_terms = self.extract_keywords_from_text(ppc_keyword)

                    for lead_kw in lead_keywords:
                        for ppc_kw in ppc_keyword_terms:
                            if FUZZY_AVAILABLE:
                                similarity = fuzz.token_sort_ratio(lead_kw, ppc_kw)
                            else:
                                similarity = 100 if lead_kw == ppc_kw else 0
                            
                            if similarity > 60:
                                # Boost score for exact matches
                                if similarity == 100:
                                    keyword_match_score = max(keyword_match_score, similarity + 10)
                                else:
                                    keyword_match_score = max(keyword_match_score, similarity)
                                matched_keywords.append((lead_kw, ppc_kw, similarity))

                # Calculate confidence score with time data
                if keyword_match_score > 0:
                    confidence_score = (0.6 * keyword_match_score) + (0.4 * time_proximity_score)
                    threshold = self.confidence_thresholds['low']

                    if confidence_score >= threshold:
                        self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                        self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score
                        self.leads_df.loc[idx, 'data_source'] = 'ppc_csv'

                        matched_kw_str = '; '.join([f"{l}-{p}" for l, p, s in matched_keywords[:3]])
                        min_hours = min(time_diffs)
                        detail = f"Keyword matches: {matched_kw_str}, Time gap: {min_hours:.1f}h, Proximity score: {time_proximity_score:.1f}% (source: ppc_csv)"
                        
                        self.leads_df.loc[idx, 'attribution_detail'] = detail
                        ppc_count += 1
                        
            else:
                # Keyword-only attribution (no time data)
                confidence_score, matched_keywords = self.match_ppc_keywords_only(lead_keywords)
                
                # Use lower threshold for keyword-only matching
                threshold = self.confidence_thresholds['low'] * 0.8

                if confidence_score >= threshold:
                    self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score
                    self.leads_df.loc[idx, 'data_source'] = 'ppc_csv'

                    matched_kw_str = '; '.join([f"{l}-{p}" for l, p, s in matched_keywords[:3]])
                    detail = f"Keyword match only (no date data): {matched_kw_str} (source: ppc_csv)"
                    
                    self.leads_df.loc[idx, 'attribution_detail'] = detail
                    ppc_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            attribution_method = "time-aware" if has_valid_dates else "keyword-only"
            print_colored(f"✓ Identified {ppc_count} leads as PPC traffic using {attribution_method} matching ({ppc_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)

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
            try:
                inquiry_date = inquiry_time.date()
                if inquiry_date in busy_dates:
                    daily_cluster_count = date_counts.get(inquiry_date, 0) - 1  # Exclude current lead
                    if daily_cluster_count > 0:
                        daily_score = min(30, daily_cluster_count * 8)
                        referral_score += daily_score
                        referral_evidence.append(f"Daily cluster: {daily_cluster_count} other leads on {inquiry_date}")
            except AttributeError:
                pass  # Skip if timestamp processing fails

            # Check hourly clusters (more precise referral detection)
            try:
                inquiry_hour = inquiry_time.floor('H')
                if inquiry_hour in busy_hours:
                # Define tighter time window for referral detection
                    time_window_start = inquiry_time - pd.Timedelta(hours=2)
                    time_window_end = inquiry_time + pd.Timedelta(hours=2)

                    # Find leads in the same time window
                    time_window_mask = (
                        (self.leads_df['first_ticket_date'] >= time_window_start) &
                        (self.leads_df['first_ticket_date'] <= time_window_end) &
                        (self.leads_df.index != idx) &
                        (self.leads_df['first_ticket_date'].notna())
                    )
                    
                    time_window_inquiries = self.leads_df[time_window_mask]
                    time_cluster_count = len(time_window_inquiries)
                    
                    if time_cluster_count > 0:
                        # Higher score for tighter time clusters
                        hourly_time_window_start = inquiry_time - pd.Timedelta(hours=1)
                        hourly_time_window_end = inquiry_time + pd.Timedelta(hours=1)
                        
                        hourly_cluster_mask = (
                            (time_window_inquiries['first_ticket_date'] >= hourly_time_window_start) &
                            (time_window_inquiries['first_ticket_date'] <= hourly_time_window_end)
                        )
                        
                        hourly_cluster_count = hourly_cluster_mask.sum()
                        
                        if hourly_cluster_count > 0:
                            time_score = min(50, hourly_cluster_count * 20)  # Higher score for tight clusters
                            referral_evidence.append(f"Tight time cluster: {hourly_cluster_count} leads within 1 hour")
                        else:
                            time_score = min(35, time_cluster_count * 12)
                            referral_evidence.append(f"Time cluster: {time_cluster_count} leads within 2 hours")
                        
                        referral_score += time_score
            except AttributeError:
                pass  # Skip if timestamp processing fails

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

    def generate_text_report(self, output_path: str = "./output/attribution_report.txt"):
        """Generate a comprehensive text report of attribution analysis"""
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                f.write("="*70 + "\n")
                f.write("TRAFFIC ATTRIBUTION ANALYSIS REPORT\n")
                f.write("="*70 + "\n")
                f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Leads Analyzed: {len(self.leads_df)}\n\n")
                
                # Attribution breakdown by source
                f.write("1. ATTRIBUTION BREAKDOWN BY SOURCE\n")
                f.write("-" * 40 + "\n")
                attribution_counts = self.leads_df['attributed_source'].value_counts()
                total_leads = len(self.leads_df)
                
                for source, count in attribution_counts.items():
                    percentage = (count / total_leads) * 100
                    f.write(f"{source:15}: {count:4d} leads ({percentage:5.1f}%)\n")
                
                f.write(f"\nTotal Attributed: {attribution_counts.sum()} leads\n")
                unknown_count = attribution_counts.get('Unknown', 0)
                if unknown_count > 0:
                    f.write(f"Attribution Rate: {((total_leads - unknown_count) / total_leads) * 100:.1f}%\n")
                
                # Confidence level distribution
                f.write("\n2. CONFIDENCE LEVEL DISTRIBUTION\n")
                f.write("-" * 40 + "\n")
                confidence_counts = self.leads_df['confidence_level'].value_counts()
                
                for level, count in confidence_counts.items():
                    percentage = (count / total_leads) * 100
                    f.write(f"{level:10}: {count:4d} leads ({percentage:5.1f}%)\n")
                
                # Top products by source
                f.write("\n3. TOP PRODUCTS BY SOURCE\n")
                f.write("-" * 40 + "\n")
                
                for source in attribution_counts.index:
                    if source == 'Unknown':
                        continue
                        
                    source_leads = self.leads_df[self.leads_df['attributed_source'] == source]
                    if len(source_leads) == 0:
                        continue
                        
                    f.write(f"\n{source} Traffic ({len(source_leads)} leads):\n")
                    
                    # Extract products from these leads
                    all_products = []
                    for _, lead in source_leads.iterrows():
                        products = lead.get('product', '')
                        if products:
                            all_products.extend([p.strip() for p in str(products).split(';') if p.strip()])
                    
                    if all_products:
                        product_counts = pd.Series(all_products).value_counts()
                        for product, count in product_counts.head(5).items():
                            f.write(f"  - {product}: {count} mentions\n")
                    else:
                        f.write("  - No specific products identified\n")
                
                # Time patterns analysis
                f.write("\n4. TIME PATTERNS\n")
                f.write("-" * 40 + "\n")
                
                # Day of week patterns
                if 'day_of_week' in self.leads_df.columns:
                    day_counts = self.leads_df['day_of_week'].value_counts()
                    f.write("Day of Week Distribution:\n")
                    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    for day in day_order:
                        if day in day_counts:
                            count = day_counts[day]
                            percentage = (count / total_leads) * 100
                            f.write(f"  {day:10}: {count:3d} leads ({percentage:4.1f}%)\n")
                
                # Timestamp analysis
                if 'first_inquiry_timestamp' in self.leads_df.columns:
                    valid_timestamps = self.leads_df['first_inquiry_timestamp'].dropna()
                    if len(valid_timestamps) > 0:
                        f.write(f"\nTimestamp Analysis ({len(valid_timestamps)} leads with valid timestamps):\n")
                        f.write(f"  Date Range: {valid_timestamps.min().strftime('%Y-%m-%d')} to {valid_timestamps.max().strftime('%Y-%m-%d')}\n")
                        
                        # Hour patterns
                        hour_counts = valid_timestamps.dt.hour.value_counts().sort_index()
                        peak_hour = hour_counts.idxmax()
                        f.write(f"  Peak Hour: {peak_hour}:00 ({hour_counts[peak_hour]} leads)\n")
                        
                        # Business hours vs after hours
                        business_hours = valid_timestamps.dt.hour.between(9, 17)
                        business_count = business_hours.sum()
                        after_hours_count = len(valid_timestamps) - business_count
                        f.write(f"  Business Hours (9-17): {business_count} leads ({(business_count/len(valid_timestamps))*100:.1f}%)\n")
                        f.write(f"  After Hours: {after_hours_count} leads ({(after_hours_count/len(valid_timestamps))*100:.1f}%)\n")
                
                # Data source breakdown
                f.write("\n5. DATA SOURCE BREAKDOWN\n")
                f.write("-" * 40 + "\n")
                if 'data_source' in self.leads_df.columns:
                    source_counts = self.leads_df['data_source'].value_counts()
                    for data_source, count in source_counts.items():
                        percentage = (count / total_leads) * 100
                        f.write(f"{data_source:15}: {count:4d} leads ({percentage:5.1f}%)\n")
                
                # Key insights
                f.write("\n6. KEY INSIGHTS\n")
                f.write("-" * 40 + "\n")
                
                # Calculate insights
                top_source = attribution_counts.index[0] if len(attribution_counts) > 0 else "Unknown"
                top_source_count = attribution_counts.iloc[0] if len(attribution_counts) > 0 else 0
                
                high_confidence_count = confidence_counts.get('High', 0)
                medium_confidence_count = confidence_counts.get('Medium', 0)
                low_confidence_count = confidence_counts.get('Low', 0)
                
                f.write(f"• Primary traffic source: {top_source} ({top_source_count} leads)\n")
                f.write(f"• High confidence attributions: {high_confidence_count} leads\n")
                f.write(f"• Attribution quality: {((high_confidence_count + medium_confidence_count) / total_leads) * 100:.1f}% medium+ confidence\n")
                
                if 'first_inquiry_timestamp' in self.leads_df.columns and len(valid_timestamps) > 0:
                    weekend_mask = valid_timestamps.dt.dayofweek.isin([5, 6])  # Saturday, Sunday
                    weekend_count = weekend_mask.sum()
                    weekday_count = len(valid_timestamps) - weekend_count
                    f.write(f"• Weekend vs Weekday: {weekend_count} weekend, {weekday_count} weekday leads\n")
                
                # Data limitations section
                f.write("\n6. DATA LIMITATIONS\n")
                f.write("-" * 40 + "\n")
                
                # Check for PPC attribution limitations
                ppc_attributed_leads = self.leads_df[self.leads_df['attributed_source'] == 'PPC']
                if len(ppc_attributed_leads) > 0:
                    keyword_only_ppc = ppc_attributed_leads[
                        ppc_attributed_leads['attribution_detail'].str.contains('keyword match only', na=False)
                    ]
                    if len(keyword_only_ppc) > 0:
                        f.write(f"• PPC Attribution Limitation: {len(keyword_only_ppc)} PPC leads attributed using keyword matching only\n")
                        f.write("  (No timestamp data available for time-based validation)\n")
                        f.write(f"  Confidence capped at 60% for these attributions\n")
                
                # Check for missing timestamp data
                missing_timestamps = self.leads_df['first_inquiry_timestamp'].isna().sum()
                if missing_timestamps > 0:
                    f.write(f"• Timestamp Data: {missing_timestamps} leads missing timestamp data\n")
                    f.write("  This limits time-based attribution accuracy\n")
                
                # Check data source diversity
                if 'data_source' in self.leads_df.columns:
                    csv_only_sources = self.leads_df['data_source'].str.contains('csv', na=False).sum()
                    if csv_only_sources > total_leads * 0.8:
                        f.write("• Data Sources: Heavily reliant on CSV data sources\n")
                        f.write("  Consider integrating live API data for real-time attribution\n")

                # Recommendations
                f.write("\n7. RECOMMENDATIONS\n")
                f.write("-" * 40 + "\n")
                
                if top_source_count > total_leads * 0.4:
                    f.write(f"• Consider diversifying traffic sources - {top_source} dominates ({(top_source_count/total_leads)*100:.1f}%)\n")
                
                if unknown_count > total_leads * 0.3:
                    f.write(f"• Improve attribution tracking - {unknown_count} leads unattributed ({(unknown_count/total_leads)*100:.1f}%)\n")
                
                if low_confidence_count > high_confidence_count:
                    f.write("• Enhance data quality - more low confidence than high confidence attributions\n")
                
                # PPC-specific recommendations
                if len(ppc_attributed_leads) > 0:
                    keyword_only_pct = (len(keyword_only_ppc) / len(ppc_attributed_leads)) * 100 if len(ppc_attributed_leads) > 0 else 0
                    if keyword_only_pct > 50:
                        f.write("• Include timestamp data in PPC reports for better attribution accuracy\n")
                
                if self.use_gsc_data:
                    f.write("• GSC integration enabled - consider expanding API data sources\n")
                else:
                    f.write("• Consider enabling Google Search Console integration for better SEO attribution\n")
                
                f.write("\n" + "="*70 + "\n")
                f.write("End of Report\n")
                f.write("="*70 + "\n")
            
            print_colored(f"✓ Text report saved to {output_path}", Colors.GREEN)
            return True
            
        except Exception as e:
            print_colored(f"Error generating text report: {e}", Colors.RED)
            return False

    def export_attribution_summary(self, output_path: str = "./output/attribution_summary.csv"):
        """Export attribution summary statistics to CSV"""
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Source counts and percentages
            attribution_counts = self.leads_df['attributed_source'].value_counts()
            total_leads = len(self.leads_df)
            
            summary_data = []
            
            for source, count in attribution_counts.items():
                source_leads = self.leads_df[self.leads_df['attributed_source'] == source]
                
                # Calculate confidence statistics
                confidence_scores = source_leads['attribution_confidence']
                avg_confidence = confidence_scores.mean() if len(confidence_scores) > 0 else 0
                min_confidence = confidence_scores.min() if len(confidence_scores) > 0 else 0
                max_confidence = confidence_scores.max() if len(confidence_scores) > 0 else 0
                
                # High confidence percentage for this source
                high_conf_count = len(source_leads[source_leads['attribution_confidence'] >= 80])
                high_conf_pct = (high_conf_count / count * 100) if count > 0 else 0
                
                # Top products for this source
                all_products = []
                for _, lead in source_leads.iterrows():
                    products = lead.get('product', '')
                    if products:
                        all_products.extend([p.strip() for p in str(products).split(';') if p.strip()])
                
                top_product = ""
                top_product_count = 0
                if all_products:
                    product_counts = pd.Series(all_products).value_counts()
                    top_product = product_counts.index[0] if len(product_counts) > 0 else ""
                    top_product_count = product_counts.iloc[0] if len(product_counts) > 0 else 0
                
                summary_data.append({
                    'source': source,
                    'lead_count': count,
                    'percentage': round((count / total_leads) * 100, 1),
                    'avg_confidence': round(avg_confidence, 1),
                    'min_confidence': round(min_confidence, 1),
                    'max_confidence': round(max_confidence, 1),
                    'high_confidence_count': high_conf_count,
                    'high_confidence_percentage': round(high_conf_pct, 1),
                    'top_product': top_product,
                    'top_product_mentions': top_product_count
                })
            
            # Create DataFrame and save
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.sort_values('lead_count', ascending=False)
            summary_df.to_csv(output_path, index=False)
            
            print_colored(f"✓ Attribution summary saved to {output_path}", Colors.GREEN)
            return True
            
        except Exception as e:
            print_colored(f"Error exporting attribution summary: {e}", Colors.RED)
            return False

    def display_progress_bar(self, current: int, total: int, description: str = "Processing"):
        """Display a simple text-based progress bar"""
        if total == 0:
            return
            
        percentage = (current / total) * 100
        bar_length = 40
        filled_length = int(bar_length * current // total)
        
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        print(f"\r{description}: |{bar}| {current}/{total} ({percentage:.1f}%)", end='', flush=True)
        
        if current == total:
            print()  # New line when complete

    def display_key_insights(self):
        """Display key insights to console with colored formatting"""
        print_colored("\n=== KEY INSIGHTS ===", Colors.BOLD + Colors.BLUE)
        
        # Attribution breakdown
        attribution_counts = self.leads_df['attributed_source'].value_counts()
        total_leads = len(self.leads_df)
        
        if len(attribution_counts) > 0:
            top_source = attribution_counts.index[0]
            top_count = attribution_counts.iloc[0]
            print_colored(f"🎯 Primary Traffic Source: {top_source} ({top_count} leads, {(top_count/total_leads)*100:.1f}%)", Colors.GREEN)
        
        # Attribution quality
        high_conf_count = len(self.leads_df[self.leads_df['attribution_confidence'] >= 80])
        medium_conf_count = len(self.leads_df[self.leads_df['attribution_confidence'] >= 50])
        quality_score = ((high_conf_count + medium_conf_count) / total_leads) * 100
        
        quality_color = Colors.GREEN if quality_score >= 70 else Colors.YELLOW if quality_score >= 50 else Colors.RED
        print_colored(f"📊 Attribution Quality: {quality_score:.1f}% medium+ confidence", quality_color)
        
        # Unknown attribution warning
        unknown_count = attribution_counts.get('Unknown', 0)
        if unknown_count > 0:
            unknown_pct = (unknown_count / total_leads) * 100
            if unknown_pct > 30:
                print_colored(f"⚠️  High Unknown Attribution: {unknown_count} leads ({unknown_pct:.1f}%) - consider improving tracking", Colors.YELLOW)
        
        # Time patterns
        if 'first_inquiry_timestamp' in self.leads_df.columns:
            valid_timestamps = self.leads_df['first_inquiry_timestamp'].dropna()
            if len(valid_timestamps) > 0:
                business_hours = valid_timestamps.dt.hour.between(9, 17)
                business_pct = (business_hours.sum() / len(valid_timestamps)) * 100
                print_colored(f"🕒 Business Hours Activity: {business_pct:.1f}% of leads during 9-17h", Colors.BLUE)
        
        # Data source diversity
        if 'data_source' in self.leads_df.columns:
            data_sources = self.leads_df['data_source'].nunique()
            print_colored(f"📈 Data Source Diversity: {data_sources} different attribution methods used", Colors.BLUE)
        
        print_colored("=" * 50, Colors.BLUE)

def analyze_traffic_attribution(leads_path="./output/leads_with_products.csv",
                              seo_csv_path=None,
                              ppc_standard_path=None,
                              ppc_dynamic_path=None,
                              output_path="./output/leads_with_attribution.csv",
                              use_gsc_data=False,
                              gsc_client=None,
                              compare_methods=False,
                              generate_reports=True):
    """Main function to run traffic attribution analysis"""
    try:
        print_colored("=== Traffic Attribution Analysis ===", Colors.BOLD + Colors.BLUE)
        
        # Initialize analyzer
        analyzer = LeadAttributionAnalyzer(
            use_gsc_data=use_gsc_data,
            gsc_client=gsc_client,
            compare_methods=compare_methods
        )
        
        # Load data with progress
        print_colored("Loading data sources...", Colors.BLUE)
        analyzer.load_data(
            leads_path=leads_path,
            seo_csv_path=seo_csv_path,
            ppc_standard_path=ppc_standard_path,
            ppc_dynamic_path=ppc_dynamic_path
        )
        
        # Run attribution with progress tracking
        print_colored("Running attribution analysis...", Colors.BLUE)
        total_leads = len(analyzer.leads_df)
        
        # Show progress for large datasets
        if total_leads > 100:
            print_colored(f"Processing {total_leads} leads...", Colors.BLUE)
        
        attributed_leads = analyzer.run_attribution()
        
        # Save main results
        print_colored("Saving results...", Colors.BLUE)
        success = analyzer.save_results(output_path)
        
        if success:
            print_colored(f"\n✓ Traffic attribution analysis completed successfully!", Colors.GREEN)
            print_colored(f"Results saved to: {output_path}", Colors.BLUE)
            
            # Generate reports if requested
            if generate_reports:
                print_colored("\nGenerating reports...", Colors.BLUE)
                
                # Progress tracking for report generation
                analyzer.display_progress_bar(1, 3, "Text Report")
                analyzer.generate_text_report()
                
                analyzer.display_progress_bar(2, 3, "Summary CSV")
                analyzer.export_attribution_summary()
                
                analyzer.display_progress_bar(3, 3, "Complete")
                
                print_colored("\n✓ Reports generated successfully!", Colors.GREEN)
                print_colored("  - Text report: ./output/attribution_report.txt", Colors.BLUE)
                print_colored("  - Summary CSV: ./output/attribution_summary.csv", Colors.BLUE)
            
            # Display key insights
            analyzer.display_key_insights()
            
            return len(attributed_leads)
        else:
            print_colored("Traffic attribution analysis completed with errors", Colors.YELLOW)
            return 0
            
    except Exception as e:
        print_colored(f"Error in traffic attribution analysis: {e}", Colors.RED)
        return 0

if __name__ == "__main__":
    analyze_traffic_attribution()
