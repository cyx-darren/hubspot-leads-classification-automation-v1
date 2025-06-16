
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
from datetime import timedelta
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
    def __init__(self, use_gsc=False, gsc_credentials_path=None, gsc_property_url=None, gsc_client=None, use_ga4=False, ga4_property_id=None, compare_methods=False):
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
        self.use_gsc = use_gsc
        self.gsc_client = gsc_client
        self.gsc_data = None
        self.gsc_property_url = gsc_property_url
        self.gsc_keywords_df = None
        
        # Google Analytics 4 integration parameters
        self.use_ga4 = use_ga4
        self.ga4_client = None
        self.ga4_traffic_data = None
        
        if use_ga4:
            self.setup_ga4_client(ga4_property_id)
        
        # Setup GSC client if credentials provided
        if use_gsc and gsc_credentials_path:
            self.setup_gsc_client(gsc_credentials_path, gsc_property_url)
        elif use_gsc and gsc_client is None:
            # Initialize GSC client if requested but not provided
            try:
                from .gsc_client import create_gsc_client
                self.gsc_client = create_gsc_client()
                if self.gsc_client:
                    print_colored("✓ GSC client auto-initialized", Colors.GREEN)
                else:
                    print_colored("Warning: Could not auto-initialize GSC client", Colors.YELLOW)
                    self.use_gsc = False
            except ImportError:
                print_colored("Warning: GSC client module not available", Colors.YELLOW)
                self.use_gsc = False
        
        # Comparison mode for testing different attribution methods
        self.compare_methods = compare_methods
    
    def check_gsc_availability(self):
        """Check if GSC is configured and available"""
        has_creds = bool(os.environ.get('GSC_CREDENTIALS')) or os.path.exists('data/gsc_credentials.json')
        has_url = bool(os.environ.get('GSC_PROPERTY_URL'))
        
        if has_creds and has_url:
            print_colored("✓ GSC configuration detected - will attempt to use real search data", Colors.GREEN)
            return True
        else:
            if not has_creds:
                print_colored("ℹ️  GSC credentials not found - using CSV data only", Colors.BLUE)
            if not has_url:
                print_colored("ℹ️  GSC_PROPERTY_URL not set - using CSV data only", Colors.BLUE)
            return False

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
        
        # Debug SEO data assignment
        if self.seo_keywords_df is not None:
            print_colored(f"SEO data loaded from traffic_loader with columns: {list(self.seo_keywords_df.columns)}", Colors.BLUE)
        else:
            print_colored("SEO data is None from traffic_loader", Colors.YELLOW)
        
        # Create fallback data if nothing was loaded
        if self.seo_keywords_df is None or self.seo_keywords_df.empty:
            print_colored("Creating mock SEO data for analysis", Colors.YELLOW)
            self.seo_keywords_df = self.create_mock_seo_data()
            
        if self.ppc_standard_df is None:
            self.ppc_standard_df = pd.DataFrame()
            
        if self.ppc_dynamic_df is None:
            self.ppc_dynamic_df = pd.DataFrame()
            
        # Final verification of SEO data structure
        if not self.seo_keywords_df.empty:
            print_colored(f"Final SEO DataFrame columns: {list(self.seo_keywords_df.columns)}", Colors.BLUE)
            if 'keyphrase' not in self.seo_keywords_df.columns:
                print_colored("CRITICAL ERROR: SEO data still missing 'keyphrase' column after all processing!", Colors.RED)
            else:
                print_colored(f"✓ SEO data verified with 'keyphrase' column present", Colors.GREEN)

        # Show traffic data summary
        summary = traffic_data['summary']
        print_colored(f"✓ Traffic data summary: {summary['seo_keywords']} SEO, {summary['ppc_standard_keywords']} PPC standard, {summary['ppc_dynamic_targets']} PPC dynamic", Colors.BLUE)

        # GSC Integration - load real search data if available
        if self.check_gsc_availability():
            try:
                from .gsc_client import GoogleSearchConsoleClient
                self.gsc_client = GoogleSearchConsoleClient()
                self.gsc_client.authenticate()
                
                # Load recent GSC data
                from datetime import datetime, timedelta
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                
                self.gsc_data = self.gsc_client.get_search_queries(start_date, end_date)
                if self.gsc_data is not None and not self.gsc_data.empty:
                    print_colored(f"✓ Loaded {len(self.gsc_data)} search queries from GSC", Colors.GREEN)
                    
                    # Merge with SEO data for enhanced attribution
                    self.enhance_seo_data_with_gsc()
                else:
                    print_colored("No recent GSC data available", Colors.YELLOW)
                    self.gsc_data = None
                
            except Exception as e:
                print_colored(f"⚠️  GSC integration failed: {e}", Colors.YELLOW)
                print_colored("  Continuing with CSV data only", Colors.BLUE)
                self.gsc_data = None
                self.gsc_client = None

        # Process and clean the data
        self.process_data()

    def load_customers_from_quickbooks(self) -> pd.DataFrame:
        """Load customer emails from QuickBooks API using existing integration"""
        try:
            # Import QuickBooks functionality from existing module
            from modules.quickbooks_domain_updater import get_quickbooks_customers, extract_customer_domains
            
            print_colored("Loading customer data from QuickBooks... (this takes 20-30 seconds)", Colors.BLUE)
            print_colored("📊 Preparing customer database for attribution analysis...", Colors.BLUE)
            
            # Get customers from QuickBooks
            customers = get_quickbooks_customers()
            
            if not customers:
                print_colored("No customers retrieved from QuickBooks", Colors.YELLOW)
                print_colored("This means 'Direct' traffic attribution won't be available", Colors.YELLOW)
                return pd.DataFrame(columns=['email'])
            
            print_colored("📧 Processing customer email addresses...", Colors.BLUE)
            
            # Extract email addresses from customers
            customer_emails = []
            total_customers = len(customers)
            
            for i, customer in enumerate(customers):
                # Show progress every 500 customers
                if i > 0 and i % 500 == 0:
                    progress_pct = (i / total_customers) * 100
                    print_colored(f"   Processing emails: {i}/{total_customers} ({progress_pct:.1f}%)", Colors.BLUE)
                
                email = customer.get('PrimaryEmailAddr', {}).get('Address', '')
                if email and '@' in email:
                    customer_emails.append(email.lower().strip())
            
            # Create DataFrame
            customers_df = pd.DataFrame({
                'email': customer_emails
            })
            
            # Remove duplicates
            original_count = len(customers_df)
            customers_df = customers_df.drop_duplicates()
            final_count = len(customers_df)
            
            if original_count != final_count:
                print_colored(f"   Removed {original_count - final_count} duplicate email addresses", Colors.BLUE)
            
            print_colored(f"✅ Customer data loaded: {final_count} customers ready for attribution", Colors.GREEN)
            
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

    def check_if_existing_customer(self, email: str, inquiry_date: datetime.datetime) -> bool:
        """
        Check if an email belongs to an existing customer in QuickBooks who was created before the inquiry date
        
        Args:
            email: Customer email address to check
            inquiry_date: Date of the inquiry to compare against customer creation date
            
        Returns:
            bool: True if customer existed before inquiry date, False otherwise
        """
        try:
            # Use optimized customer loading if not already loaded
            if not hasattr(self, 'customer_attribution_map'):
                from modules.quickbooks_domain_updater import load_all_customers_for_attribution, convert_qb_date_to_datetime
                
                print_colored("🚀 Loading customers for attribution (one-time setup)...", Colors.BLUE)
                self.customer_attribution_map = load_all_customers_for_attribution()
                self.convert_qb_date_func = convert_qb_date_to_datetime
            
            if not self.customer_attribution_map:
                print_colored("No customer data available for attribution", Colors.YELLOW)
                return False
            
            # Normalize email for comparison
            email_to_check = email.lower().strip()
            
            print_colored(f"🔍 Checking customer status: {email_to_check} (inquiry: {inquiry_date.strftime('%Y-%m-%d %H:%M')})", Colors.BLUE)
            
            # Look up customer in the pre-loaded map
            if email_to_check not in self.customer_attribution_map:
                print_colored(f"Customer {email_to_check} not found in QuickBooks", Colors.BLUE)
                return False
            
            # Get creation date from map
            creation_date_str = self.customer_attribution_map[email_to_check]
            
            if not creation_date_str:
                print_colored(f"Customer {email_to_check} found but no creation date available", Colors.YELLOW)
                return False
            
            # Convert QuickBooks date to datetime
            creation_date = self.convert_qb_date_func(creation_date_str)
            
            if creation_date is None:
                print_colored(f"Could not parse creation date for customer {email_to_check}: {creation_date_str}", Colors.YELLOW)
                return False
            
            # Ensure both dates are timezone-aware for comparison
            from datetime import timezone
            
            if inquiry_date.tzinfo is None:
                inquiry_date = inquiry_date.replace(tzinfo=timezone.utc)
            
            if creation_date.tzinfo is None:
                creation_date = creation_date.replace(tzinfo=timezone.utc)
            
            # Check if customer was created before inquiry
            is_existing = creation_date < inquiry_date
            
            time_diff = inquiry_date - creation_date
            print_colored(
                f"✓ {email_to_check}: Created {creation_date.strftime('%Y-%m-%d %H:%M')}, "
                f"Inquiry {inquiry_date.strftime('%Y-%m-%d %H:%M')}, "
                f"Gap: {time_diff.days} days, "
                f"Existing: {is_existing}", 
                Colors.GREEN if is_existing else Colors.BLUE
            )
            
            return is_existing
            
        except ImportError:
            print_colored("QuickBooks module not available for customer checking", Colors.YELLOW)
            return False
        except Exception as e:
            print_colored(f"Error checking customer status for {email}: {e}", Colors.RED)
            print_colored(f"This could be due to QuickBooks API issues or token expiration", Colors.YELLOW)
            return False

    def load_seo_data_from_csv(self, file_path: str) -> pd.DataFrame:
        """Load SEO keyword data from CSV file"""
        try:
            seo_df = pd.read_csv(file_path)
            print(f"   Raw SEO columns before rename: {list(seo_df.columns)}")
            
            # Rename columns - ensure this actually happens
            rename_dict = {
                'Keyphrase': 'keyphrase',
                'Current Page': 'current_page',
                'Current Position': 'current_position'
            }
            
            seo_df = seo_df.rename(columns=rename_dict)
            print(f"   SEO columns after rename: {list(seo_df.columns)}")
            
            # Verify the rename worked
            if 'keyphrase' not in seo_df.columns:
                print(f"   ERROR: 'keyphrase' column still missing after rename!")
                print(f"   Available columns: {list(seo_df.columns)}")
                return pd.DataFrame()  # Return empty DataFrame
            
            # Convert position to numeric (it's already integer but ensure)
            if 'current_position' in seo_df.columns:
                seo_df['current_position'] = pd.to_numeric(seo_df['current_position'], errors='coerce')
                seo_df['current_position'] = seo_df['current_position'].fillna(100)
            else:
                print(f"   Warning: 'current_position' column missing, using default value")
                seo_df['current_position'] = 100
            
            # Add product category based on keyphrase
            seo_df['product_category'] = seo_df['keyphrase'].apply(self.extract_product_category_from_keyword)
            
            print_colored(f"   ✓ Loaded {len(seo_df)} SEO keywords with positions", Colors.GREEN)
            
            return seo_df
            
        except Exception as e:
            print_colored(f"   Error in load_seo_data_from_csv: {e}", Colors.RED)
            import traceback
            traceback.print_exc()
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
        
        print_colored(f"   ✓ Created mock SEO data with columns: {list(seo_df.columns)}", Colors.BLUE)
        
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
        if self.use_gsc and self.gsc_client:
            print_colored("GSC integration enabled - loading click data", Colors.BLUE)
            try:
                # Load GSC data for attribution window based on lead timestamps
                if hasattr(self, 'leads_df') and self.leads_df is not None and 'first_ticket_date' in self.leads_df.columns:
                    # Calculate date range based on actual lead data
                    valid_dates = self.leads_df['first_ticket_date'].dropna()
                    if len(valid_dates) > 0:
                        earliest_lead = valid_dates.min()
                        latest_lead = valid_dates.max()
                        date_span = (latest_lead - earliest_lead).days
                        days_back = max(30, date_span + 7)  # At least 30 days, or lead span + buffer
                    else:
                        days_back = 30
                else:
                    days_back = 30
                
                self.gsc_data = self.load_gsc_data(days_back=days_back)
                self.gsc_click_data = self.gsc_data  # For compatibility
                
                if self.gsc_data is not None:
                    print_colored(f"✓ Loaded {len(self.gsc_data)} GSC click records", Colors.GREEN)
                else:
                    print_colored("No GSC data loaded", Colors.YELLOW)
                    self.gsc_click_data = pd.DataFrame()
            except Exception as e:
                print_colored(f"Warning: Could not load GSC data: {e}", Colors.YELLOW)
                self.gsc_click_data = pd.DataFrame()
        else:
            print_colored("GSC integration disabled - using CSV fallback", Colors.BLUE)
            self.gsc_click_data = pd.DataFrame()

    def setup_gsc_client(self, credentials_path: str, property_url: str = None):
        """Setup Google Search Console client with credentials"""
        try:
            from .gsc_client import GoogleSearchConsoleClient
            
            self.gsc_client = GoogleSearchConsoleClient(credentials_path=credentials_path)
            
            if self.gsc_client.authenticate(property_url or self.gsc_property_url):
                print_colored("✓ GSC client authenticated successfully", Colors.GREEN)
                self.gsc_property_url = property_url or self.gsc_property_url
            else:
                print_colored("Warning: GSC authentication failed", Colors.YELLOW)
                self.gsc_client = None
                self.use_gsc = False
                
        except ImportError:
            print_colored("Warning: GSC client module not available", Colors.YELLOW)
            self.gsc_client = None
            self.use_gsc = False
        except Exception as e:
            print_colored(f"Error setting up GSC client: {e}", Colors.RED)
            self.gsc_client = None
            self.use_gsc = False

    def setup_ga4_client(self, property_id=None):
        """Setup GA4 client for pattern matching"""
        try:
            from .ga4_client import GoogleAnalytics4Client
            
            self.ga4_client = GoogleAnalytics4Client(property_id=property_id)
            if self.ga4_client.authenticate():
                print_colored("✓ GA4 client authenticated successfully", Colors.GREEN)
                self.use_ga4 = True
            else:
                print_colored("✗ GA4 authentication failed", Colors.YELLOW)
                self.use_ga4 = False
        except ImportError:
            print_colored("Warning: GA4 client module not available", Colors.YELLOW)
            self.use_ga4 = False
        except Exception as e:
            print_colored(f"✗ Could not setup GA4 client: {e}", Colors.YELLOW)
            self.use_ga4 = False
    
    def enhance_seo_data_with_gsc(self):
        """Enhance SEO keywords with actual click data from GSC"""
        if self.gsc_data is None or self.gsc_data.empty:
            return
        
        print_colored("Enhancing SEO data with real GSC click information...", Colors.BLUE)
        
        # Add click data to SEO keywords
        click_summary = self.gsc_data.groupby('query').agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'position': 'mean'
        }).reset_index()
        
        # Create enhanced keyword list with actual performance
        self.gsc_keywords_df = click_summary.copy()
        self.gsc_keywords_df['has_clicks'] = self.gsc_keywords_df['clicks'] > 0
        self.gsc_keywords_df['keyphrase'] = self.gsc_keywords_df['query']  # For compatibility
        self.gsc_keywords_df['current_position'] = self.gsc_keywords_df['position']
        
        # Add product category mapping
        self.gsc_keywords_df['product_category'] = self.gsc_keywords_df['query'].apply(
            self.extract_product_category_from_keyword
        )
        
        print_colored(f"✓ Enhanced with {len(self.gsc_keywords_df)} keywords from GSC", Colors.GREEN)
        print_colored(f"  - {self.gsc_keywords_df['has_clicks'].sum()} keywords have actual clicks", Colors.GREEN)
        
        # Set flag to prioritize GSC data in attribution
        self.use_gsc = True
    
    def load_gsc_data(self, property_url: str = None, days_back: int = 30) -> pd.DataFrame:
        """Load actual search performance from GSC"""
        if not self.gsc_client:
            print_colored("No GSC client available for data loading", Colors.YELLOW)
            return None
            
        try:
            from datetime import datetime, timedelta
            
            # Get data for attribution window
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            print_colored(f"Loading GSC data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}", Colors.BLUE)
            
            # Fetch search queries with clicks
            self.gsc_data = self.gsc_client.get_search_queries(start_date, end_date, limit=2000)
            
            if self.gsc_data is not None and not self.gsc_data.empty:
                total_clicks = self.gsc_data['clicks'].sum()
                print_colored(f"✓ Loaded {len(self.gsc_data)} search queries from GSC with {total_clicks} total clicks", Colors.GREEN)
                
                # Add some processing for better attribution
                self.gsc_data['query_lower'] = self.gsc_data['query'].str.lower()
                self.gsc_data['query_words'] = self.gsc_data['query_lower'].apply(
                    lambda x: x.split() if isinstance(x, str) else []
                )
                
                return self.gsc_data
            else:
                print_colored("No GSC data retrieved", Colors.YELLOW)
                return None
                
        except Exception as e:
            print_colored(f"Error loading GSC data: {e}", Colors.RED)
            return None

    def get_gsc_click_data(self) -> pd.DataFrame:
        """
        Get click data from Google Search Console API
        Now properly implemented with GSC integration
        """
        if self.gsc_data is not None:
            return self.gsc_data
        
        # Try to load data if client is available
        if self.gsc_client:
            return self.load_gsc_data()
        
        print_colored("GSC client not available - returning empty DataFrame", Colors.YELLOW)
        return pd.DataFrame(columns=['date', 'query', 'clicks', 'impressions', 'position', 'page'])

    def load_ga4_traffic_patterns(self):
        """Load traffic patterns from GA4 for validation"""
        if not self.ga4_client:
            print_colored("GA4 client not available for pattern loading", Colors.YELLOW)
            return None
            
        try:
            # Get date range based on leads
            valid_timestamps = self.leads_df['first_ticket_date'].dropna()
            if len(valid_timestamps) == 0:
                print_colored("No valid lead timestamps for GA4 date range", Colors.YELLOW)
                return None
                
            min_date = valid_timestamps.min()
            max_date = valid_timestamps.max()
            
            # Add buffer days
            start_date = min_date - pd.Timedelta(days=7)
            end_date = max_date + pd.Timedelta(days=1)
            
            print_colored(f"Loading GA4 traffic data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}", Colors.BLUE)
            
            # Get hourly traffic patterns
            self.ga4_traffic_data = self.ga4_client.get_hourly_traffic_patterns(
                start_date, end_date
            )
            
            if self.ga4_traffic_data is not None and not self.ga4_traffic_data.empty:
                print_colored(f"✓ Loaded {len(self.ga4_traffic_data)} GA4 traffic records", Colors.GREEN)
                
                # Show traffic summary
                traffic_summary = self.ga4_traffic_data.groupby('medium')['sessions'].sum().sort_values(ascending=False)
                print_colored("\nGA4 Traffic Summary:", Colors.BLUE)
                for medium, sessions in traffic_summary.head().items():
                    print_colored(f"  - {medium}: {sessions} sessions", Colors.BLUE)
                
                # Specifically highlight PPC traffic
                ppc_sessions = traffic_summary[traffic_summary.index.isin(['cpc', 'ppc', 'paid'])].sum()
                if ppc_sessions > 0:
                    print_colored(f"\n✓ Found {ppc_sessions} PPC sessions in GA4 data - will use for attribution", Colors.GREEN)
            else:
                print_colored("No GA4 traffic data available for the period", Colors.YELLOW)
            
            return self.ga4_traffic_data
            
        except Exception as e:
            print_colored(f"Error loading GA4 data: {e}", Colors.RED)
            return None

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
                return
            
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
                return
            
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
        
        total_steps = 6 if self.use_ga4 else 5
        
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

        # Step 5: GA4 validation (if enabled)
        if self.use_ga4:
            self.display_progress_bar(5, total_steps, "GA4 Validation")
            self.load_ga4_traffic_patterns()
            self.validate_attribution_with_ga4()

        # Step 6: Calculate confidence scores and finalize attribution
        self.display_progress_bar(total_steps, total_steps, "Finalizing")
        self.finalize_attribution()

        print_colored("\n✓ Attribution analysis completed", Colors.GREEN)
        return self.leads_df

    def identify_direct_traffic(self):
        """Identify direct traffic from returning customers using QuickBooks customer creation dates"""
        print_colored("Identifying direct traffic using QuickBooks customer verification...", Colors.BLUE)

        # Load customer cache ONCE at the beginning
        try:
            from modules.quickbooks_domain_updater import load_all_customers_for_attribution, convert_qb_date_to_datetime
            customer_cache = load_all_customers_for_attribution()
            print_colored(f"Using pre-loaded cache of {len(customer_cache)} customers for attribution", Colors.BLUE)
        except ImportError:
            print_colored("QuickBooks module not available - continuing without customer data", Colors.YELLOW)
            customer_cache = {}
        except Exception as e:
            print_colored(f"Error loading customer cache: {e}", Colors.YELLOW)
            customer_cache = {}

        if not customer_cache:
            print_colored("No customer data available for direct traffic attribution", Colors.YELLOW)
            return

        direct_count = 0
        returning_customer_count = 0
        new_conversion_count = 0
        fallback_count = 0

        # Process each lead individually with date-based customer verification
        for idx, lead in self.leads_df.iterrows():
            email = lead.get('email', '')
            inquiry_timestamp = lead.get('first_inquiry_timestamp')
            
            # Skip if no email or timestamp
            if not email or pd.isna(inquiry_timestamp):
                print_colored(f"Skipping lead {idx}: missing email or timestamp", Colors.YELLOW)
                continue
            
            # Normalize email for comparison
            email_to_check = email.lower().strip()
            
            # Log the date comparison being made
            inquiry_date_str = inquiry_timestamp.strftime('%Y-%m-%d %H:%M') if pd.notna(inquiry_timestamp) else 'Unknown'
            print_colored(f"Checking customer status for {email} at inquiry time: {inquiry_date_str}", Colors.BLUE)
            
            # Check against customer cache
            try:
                if email_to_check not in customer_cache:
                    print_colored(f"Customer {email_to_check} not found in QuickBooks", Colors.BLUE)
                    continue
                
                # Get creation date from cache
                creation_date_str = customer_cache[email_to_check]
                
                if not creation_date_str:
                    print_colored(f"Customer {email_to_check} found but no creation date available", Colors.YELLOW)
                    continue
                
                # Convert QuickBooks date to datetime
                creation_date = convert_qb_date_to_datetime(creation_date_str)
                
                if creation_date is None:
                    print_colored(f"Could not parse creation date for customer {email_to_check}: {creation_date_str}", Colors.YELLOW)
                    continue
                
                # Ensure both dates are timezone-aware for comparison
                from datetime import timezone
                
                if inquiry_timestamp.tzinfo is None:
                    inquiry_timestamp = inquiry_timestamp.replace(tzinfo=timezone.utc)
                
                if creation_date.tzinfo is None:
                    creation_date = creation_date.replace(tzinfo=timezone.utc)
                
                # Check if customer was created before inquiry
                is_existing_customer = creation_date < inquiry_timestamp
                
                time_diff = inquiry_timestamp - creation_date
                print_colored(
                    f"✓ {email_to_check}: Created {creation_date.strftime('%Y-%m-%d %H:%M')}, "
                    f"Inquiry {inquiry_timestamp.strftime('%Y-%m-%d %H:%M')}, "
                    f"Gap: {time_diff.days} days, "
                    f"Existing: {is_existing_customer}", 
                    Colors.GREEN if is_existing_customer else Colors.BLUE
                )
                
                if is_existing_customer:
                    # Customer existed BEFORE inquiry - this is genuine direct traffic
                    self.leads_df.loc[idx, 'attributed_source'] = 'Direct'
                    self.leads_df.loc[idx, 'attribution_confidence'] = 95
                    self.leads_df.loc[idx, 'attribution_detail'] = f'Verified returning customer (existed before {inquiry_date_str})'
                    self.leads_df.loc[idx, 'data_source'] = 'quickbooks_verified'
                    
                    direct_count += 1
                    returning_customer_count += 1
                    
                    print_colored(f"  ✓ {email} confirmed as returning customer", Colors.GREEN)
                    
                else:
                    # Customer was NOT found or was created after inquiry
                    # This could be a new lead that later converted, not direct traffic
                    print_colored(f"  → {email} not an existing customer at inquiry time", Colors.BLUE)
                    # Do not mark as Direct traffic - leave for other attribution methods
                    
            except Exception as e:
                # Enhanced checking failed - try fallback check against customer email list
                print_colored(f"Cache verification failed for {email}: {e}", Colors.YELLOW)
                
                # Fallback: basic email list check (less reliable)
                if hasattr(self, 'customer_emails') and email in self.customer_emails:
                    print_colored(f"  → Using fallback customer list check for {email}", Colors.YELLOW)
                    
                    self.leads_df.loc[idx, 'attributed_source'] = 'Direct'
                    self.leads_df.loc[idx, 'attribution_confidence'] = 60  # Lower confidence due to no date verification
                    self.leads_df.loc[idx, 'attribution_detail'] = f'Customer email match (cache verification failed at {inquiry_date_str})'
                    self.leads_df.loc[idx, 'data_source'] = 'customer_db_fallback'
                    
                    direct_count += 1
                    fallback_count += 1
                    
                    print_colored(f"  ✓ {email} found in customer list (fallback method)", Colors.YELLOW)
                else:
                    print_colored(f"  → {email} not found in any customer records", Colors.BLUE)

        # Summary logging
        print_colored(f"✓ Direct traffic identification completed:", Colors.GREEN)
        print_colored(f"  - Total Direct leads: {direct_count} ({direct_count/len(self.leads_df)*100:.1f}%)", Colors.GREEN)
        
        if returning_customer_count > 0:
            print_colored(f"  - Verified returning customers: {returning_customer_count}", Colors.GREEN)
        
        if new_conversion_count > 0:
            print_colored(f"  - New customer conversions: {new_conversion_count}", Colors.BLUE)
            
        if fallback_count > 0:
            print_colored(f"  - Fallback attributions: {fallback_count} (cache verification failed)", Colors.YELLOW)
            
        if direct_count == 0:
            print_colored("  - No direct traffic identified - all leads appear to be new prospects", Colors.BLUE)

    def identify_seo_traffic(self):
        """Identify traffic from SEO using GSC data first, then CSV fallback"""
        print_colored("Identifying SEO traffic...", Colors.BLUE)
        
        # First try enhanced GSC data (real clicks with keyword matching)
        if self.gsc_keywords_df is not None and not self.gsc_keywords_df.empty:
            print_colored("Using enhanced GSC data for SEO attribution (real click data)", Colors.BLUE)
            seo_count = self.attribute_using_enhanced_gsc_data()
            self._update_data_source_for_seo('gsc_enhanced')
        # Fall back to raw GSC data
        elif self.gsc_data is not None and not self.gsc_data.empty:
            print_colored("Using raw GSC data for SEO attribution (real click data)", Colors.BLUE)
            seo_count = self.attribute_using_gsc_data()
            self._update_data_source_for_seo('gsc_api')
        # Fall back to CSV ranking data
        else:
            print_colored("Using CSV data for SEO attribution (ranking data)", Colors.BLUE)
            
            if self.seo_keywords_df is None or self.seo_keywords_df.empty:
                print_colored("No SEO data available - skipping SEO attribution", Colors.YELLOW)
                return
                
            seo_count = self.attribute_using_seo_csv()
            self._update_data_source_for_seo('seo_csv')

        # If comparison mode is enabled, run both methods
        if self.compare_methods and self.seo_keywords_df is not None and not self.seo_keywords_df.empty and self.gsc_data is not None and not self.gsc_data.empty:
            print_colored("Comparison mode: Running both CSV and GSC attribution methods", Colors.BLUE)
            
            # Store current results as primary method
            current_seo_mask = self.leads_df['attributed_source'] == 'SEO'
            primary_method = 'gsc' if self.gsc_keywords_df is not None and not self.gsc_keywords_df.empty else 'csv'
            
            self.leads_df.loc[current_seo_mask, f'{primary_method}_attribution'] = 'SEO'
            self.leads_df.loc[current_seo_mask, f'{primary_method}_confidence'] = self.leads_df.loc[current_seo_mask, 'attribution_confidence']
            
            # Reset and run secondary method
            self.leads_df.loc[current_seo_mask, 'attributed_source'] = 'Unknown'
            self.leads_df.loc[current_seo_mask, 'attribution_confidence'] = 0
            
            # Run the other method
            if primary_method == 'gsc':
                secondary_count = self.attribute_using_seo_csv()
                secondary_method = 'csv'
            else:
                secondary_count = self.attribute_using_enhanced_gsc_data() if self.gsc_keywords_df is not None else self.attribute_using_gsc_data()
                secondary_method = 'gsc'
            
            # Store secondary results
            secondary_seo_mask = self.leads_df['attributed_source'] == 'SEO'
            self.leads_df.loc[secondary_seo_mask, f'{secondary_method}_attribution'] = 'SEO'
            self.leads_df.loc[secondary_seo_mask, f'{secondary_method}_confidence'] = self.leads_df.loc[secondary_seo_mask, 'attribution_confidence']

    def _identify_seo_from_gsc(self) -> int:
        """
        Identify SEO traffic using Google Search Console data
        Uses real click data instead of just ranking positions
        """
        if not self.gsc_client:
            print_colored("GSC client not available for real-time attribution", Colors.YELLOW)
            return 0
            
        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        seo_count = 0

        # Get date range for GSC data based on lead timestamps
        if 'first_inquiry_timestamp' in self.leads_df.columns:
            valid_timestamps = self.leads_df['first_inquiry_timestamp'].dropna()
            if len(valid_timestamps) > 0:
                min_date = valid_timestamps.min()
                max_date = valid_timestamps.max()
                
                # Extend range slightly for attribution window
                start_date = min_date - pd.Timedelta(days=2)
                end_date = max_date + pd.Timedelta(days=1)
            else:
                # Fallback to recent period
                end_date = datetime.datetime.now()
                start_date = end_date - datetime.timedelta(days=30)
        else:
            # Fallback to recent period
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=30)

        # Get GSC click data for the period
        print_colored(f"Fetching GSC data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}", Colors.BLUE)
        gsc_data = self.gsc_client.get_search_queries(start_date, end_date, limit=2000)
        
        if gsc_data is None or gsc_data.empty:
            print_colored("No GSC data available for attribution", Colors.YELLOW)
            return 0

        print_colored(f"✓ Retrieved {len(gsc_data)} search queries with {gsc_data['clicks'].sum()} total clicks", Colors.GREEN)

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Match lead keywords with GSC queries
            keyword_match_score = 0
            matched_queries = []
            total_clicks = 0
            total_impressions = 0
            best_position = 100

            for _, gsc_row in gsc_data.iterrows():
                gsc_query = str(gsc_row['query']).lower()
                gsc_clicks = gsc_row['clicks']
                gsc_impressions = gsc_row['impressions']
                gsc_position = gsc_row['position']
                
                # Skip queries with no clicks
                if gsc_clicks == 0:
                    continue

                for lead_kw in lead_keywords:
                    # Use fuzzy matching if available
                    if FUZZY_AVAILABLE:
                        similarity = fuzz.token_sort_ratio(lead_kw, gsc_query)
                    else:
                        similarity = 100 if lead_kw in gsc_query else 0
                    
                    if similarity > 60:  # Match threshold
                        # Weight by actual clicks (real traffic evidence)
                        click_weight = min(100, gsc_clicks * 10)  # Scale clicks to 0-100
                        position_bonus = max(0, (20 - gsc_position) * 2)  # Better positions get bonus
                        
                        match_score = (similarity * 0.4) + (click_weight * 0.4) + (position_bonus * 0.2)
                        
                        keyword_match_score = max(keyword_match_score, match_score)
                        matched_queries.append((lead_kw, gsc_query, similarity, gsc_clicks))
                        total_clicks += gsc_clicks
                        total_impressions += gsc_impressions
                        best_position = min(best_position, gsc_position)

            # Calculate GSC-based confidence score
            if keyword_match_score > 0 and total_clicks > 0:
                # Base score from keyword matching
                base_confidence = keyword_match_score
                
                # Boost confidence based on actual clicks (real traffic evidence)
                click_confidence_boost = min(30, total_clicks * 5)  # Up to 30 points for clicks
                
                # Position quality bonus
                if best_position <= 3:
                    position_bonus = 20
                elif best_position <= 10:
                    position_bonus = 10
                elif best_position <= 20:
                    position_bonus = 5
                else:
                    position_bonus = 0

                # Time proximity bonus (if we have timestamp data)
                time_bonus = 0
                if 'first_inquiry_timestamp' in lead and pd.notna(lead['first_inquiry_timestamp']):
                    lead_date = lead['first_inquiry_timestamp']
                    # Check if GSC data date is close to lead date
                    if 'date' in gsc_data.columns:
                        # Simple time proximity check (same week gets bonus)
                        time_diff_days = abs((lead_date - pd.Timestamp(start_date)).days)
                        if time_diff_days <= 7:
                            time_bonus = 15
                        elif time_diff_days <= 14:
                            time_bonus = 10

                # Final confidence calculation
                confidence_score = base_confidence + click_confidence_boost + position_bonus + time_bonus
                confidence_score = min(100, confidence_score)  # Cap at 100

                # Use higher threshold for GSC attribution since we have real click data
                threshold = self.confidence_thresholds['medium']  # 50% threshold

                if confidence_score >= threshold:
                    self.leads_df.loc[idx, 'attributed_source'] = 'SEO'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score

                    matched_queries_str = '; '.join([f"{l}-{g}({c} clicks)" for l, g, s, c in matched_queries[:3]])
                    detail = f"GSC matches: {matched_queries_str}, Total clicks: {total_clicks}, Best position: {best_position:.1f}"
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    seo_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {seo_count} leads as SEO traffic using GSC data ({seo_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)
        
        return seo_count

    def attribute_using_enhanced_gsc_data(self) -> int:
        """Attribute using enhanced GSC data with click summaries"""
        if self.gsc_keywords_df is None or self.gsc_keywords_df.empty:
            print_colored("No enhanced GSC data available for attribution", Colors.YELLOW)
            return 0
            
        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        seo_count = 0

        print_colored(f"Analyzing {unattributed_mask.sum()} unattributed leads against {len(self.gsc_keywords_df)} enhanced GSC keywords", Colors.BLUE)

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Match lead keywords with enhanced GSC data
            keyword_match_score = 0
            matched_queries = []
            total_clicks = 0
            total_impressions = 0
            best_position = 100
            has_actual_clicks = False

            for _, gsc_row in self.gsc_keywords_df.iterrows():
                gsc_query = str(gsc_row['query']).lower()
                gsc_clicks = gsc_row['clicks']
                gsc_impressions = gsc_row['impressions']
                gsc_position = gsc_row['position']
                
                # Prioritize queries with actual clicks
                if gsc_clicks > 0:
                    has_actual_clicks = True

                for lead_kw in lead_keywords:
                    # Use fuzzy matching if available
                    if FUZZY_AVAILABLE:
                        similarity = fuzz.token_sort_ratio(lead_kw, gsc_query)
                    else:
                        similarity = 100 if lead_kw in gsc_query else 0
                    
                    if similarity > 60:  # Match threshold
                        # Much higher weight for queries with actual clicks
                        if gsc_clicks > 0:
                            click_weight = min(100, gsc_clicks * 20)  # Even higher weight for enhanced data
                            position_bonus = max(0, (20 - gsc_position) * 4)
                            match_score = (similarity * 0.2) + (click_weight * 0.6) + (position_bonus * 0.2)
                        else:
                            # Lower weight for impression-only queries
                            impression_weight = min(50, gsc_impressions / 100)
                            position_bonus = max(0, (20 - gsc_position) * 2)
                            match_score = (similarity * 0.4) + (impression_weight * 0.3) + (position_bonus * 0.3)
                        
                        keyword_match_score = max(keyword_match_score, match_score)
                        matched_queries.append((lead_kw, gsc_query, similarity, gsc_clicks, gsc_position))
                        total_clicks += gsc_clicks
                        total_impressions += gsc_impressions
                        best_position = min(best_position, gsc_position)

            # Calculate enhanced GSC-based confidence score
            if keyword_match_score > 0:
                # Base score from keyword matching
                base_confidence = keyword_match_score
                
                # Major boost for actual clicks (real traffic evidence)
                if has_actual_clicks and total_clicks > 0:
                    click_confidence_boost = min(50, total_clicks * 10)  # Up to 50 points for clicks
                    data_quality_bonus = 20  # Bonus for having real click data
                else:
                    click_confidence_boost = min(20, total_impressions / 100)  # Lower boost for impressions only
                    data_quality_bonus = 5
                
                # Position quality bonus
                if best_position <= 3:
                    position_bonus = 30
                elif best_position <= 10:
                    position_bonus = 20
                elif best_position <= 20:
                    position_bonus = 10
                else:
                    position_bonus = 0

                # Final confidence calculation - much higher for enhanced data
                confidence_score = base_confidence + click_confidence_boost + position_bonus + data_quality_bonus
                confidence_score = min(100, confidence_score)

                # Use medium threshold for enhanced GSC data
                threshold = self.confidence_thresholds['medium']  # 50% threshold

                if confidence_score >= threshold:
                    self.leads_df.loc[idx, 'attributed_source'] = 'SEO'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score

                    # Create detailed attribution description
                    top_matches = sorted(matched_queries, key=lambda x: x[3], reverse=True)[:3]  # Sort by clicks
                    matched_queries_str = '; '.join([f"{l}→{g}({c} clicks, pos {p:.1f})" for l, g, s, c, p in top_matches])
                    
                    if has_actual_clicks:
                        detail = f"Enhanced GSC (with clicks): {matched_queries_str} | Total: {total_clicks} clicks, {total_impressions} impr, Best pos: {best_position:.1f}"
                    else:
                        detail = f"Enhanced GSC (impressions): {matched_queries_str} | Total: {total_impressions} impr, Best pos: {best_position:.1f}"
                    
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    seo_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {seo_count} leads as SEO traffic using enhanced GSC data ({seo_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)
        
        return seo_count

    def attribute_using_gsc_data(self) -> int:
        """Attribute using actual GSC click data"""
        if self.gsc_data is None or self.gsc_data.empty:
            print_colored("No GSC data available for attribution", Colors.YELLOW)
            return 0
            
        # Only consider leads not already attributed
        unattributed_mask = self.leads_df['attributed_source'] == 'Unknown'
        seo_count = 0

        print_colored(f"Analyzing {unattributed_mask.sum()} unattributed leads against {len(self.gsc_data)} GSC queries", Colors.BLUE)

        # Loop through unattributed leads
        for idx, lead in self.leads_df[unattributed_mask].iterrows():
            lead_keywords = lead.get('extracted_keywords', [])
            
            if not lead_keywords:
                continue

            # Match lead keywords with GSC queries
            keyword_match_score = 0
            matched_queries = []
            total_clicks = 0
            total_impressions = 0
            best_position = 100
            best_ctr = 0

            for _, gsc_row in self.gsc_data.iterrows():
                gsc_query = str(gsc_row['query']).lower()
                gsc_clicks = gsc_row['clicks']
                gsc_impressions = gsc_row['impressions']
                gsc_position = gsc_row['position']
                gsc_ctr = gsc_row.get('ctr', 0)
                
                # Skip queries with no clicks (no real traffic evidence)
                if gsc_clicks == 0:
                    continue

                for lead_kw in lead_keywords:
                    # Use fuzzy matching if available
                    if FUZZY_AVAILABLE:
                        similarity = fuzz.token_sort_ratio(lead_kw, gsc_query)
                    else:
                        similarity = 100 if lead_kw in gsc_query else 0
                    
                    if similarity > 60:  # Match threshold
                        # Weight heavily by actual clicks (real traffic evidence)
                        click_weight = min(100, gsc_clicks * 15)  # Scale clicks to 0-100, higher weight than before
                        position_bonus = max(0, (20 - gsc_position) * 3)  # Better positions get bonus
                        ctr_bonus = min(20, gsc_ctr * 200)  # CTR bonus (ctr is decimal, so *200 for percentage)
                        
                        # Higher weight on clicks since this is real traffic
                        match_score = (similarity * 0.3) + (click_weight * 0.5) + (position_bonus * 0.15) + (ctr_bonus * 0.05)
                        
                        keyword_match_score = max(keyword_match_score, match_score)
                        matched_queries.append((lead_kw, gsc_query, similarity, gsc_clicks, gsc_position))
                        total_clicks += gsc_clicks
                        total_impressions += gsc_impressions
                        best_position = min(best_position, gsc_position)
                        best_ctr = max(best_ctr, gsc_ctr)

            # Calculate GSC-based confidence score
            if keyword_match_score > 0 and total_clicks > 0:
                # Base score from keyword matching
                base_confidence = keyword_match_score
                
                # Significant boost for actual clicks (real traffic evidence)
                click_confidence_boost = min(40, total_clicks * 8)  # Up to 40 points for clicks
                
                # Position quality bonus
                if best_position <= 3:
                    position_bonus = 25
                elif best_position <= 10:
                    position_bonus = 15
                elif best_position <= 20:
                    position_bonus = 8
                else:
                    position_bonus = 0

                # Time proximity bonus (if we have timestamp data)
                time_bonus = 0
                if 'first_inquiry_timestamp' in lead and pd.notna(lead['first_inquiry_timestamp']):
                    lead_date = lead['first_inquiry_timestamp']
                    # Check if GSC data overlaps with lead timing
                    if 'date' in self.gsc_data.columns:
                        # GSC data covers the period, give time bonus
                        time_bonus = 10

                # CTR bonus for high-performing queries
                ctr_bonus = min(10, best_ctr * 100)  # Up to 10 points for high CTR

                # Final confidence calculation - much higher than CSV-only
                confidence_score = base_confidence + click_confidence_boost + position_bonus + time_bonus + ctr_bonus
                confidence_score = min(100, confidence_score)  # Cap at 100

                # Use medium threshold since we have real click data
                threshold = self.confidence_thresholds['medium']  # 50% threshold

                if confidence_score >= threshold:
                    self.leads_df.loc[idx, 'attributed_source'] = 'SEO'
                    self.leads_df.loc[idx, 'attribution_confidence'] = confidence_score

                    # Create detailed attribution description
                    top_matches = sorted(matched_queries, key=lambda x: x[3], reverse=True)[:3]  # Sort by clicks
                    matched_queries_str = '; '.join([f"{l}→{g}({c} clicks, pos {p:.1f})" for l, g, s, c, p in top_matches])
                    detail = f"GSC real clicks: {matched_queries_str} | Total: {total_clicks} clicks, {total_impressions} impr, Best pos: {best_position:.1f}, CTR: {best_ctr:.1%}"
                    self.leads_df.loc[idx, 'attribution_detail'] = detail

                    seo_count += 1

        unattributed_count = unattributed_mask.sum()
        if unattributed_count > 0:
            print_colored(f"✓ Identified {seo_count} leads as SEO traffic using GSC click data ({seo_count/unattributed_count*100:.1f}% of unattributed)", Colors.GREEN)
        
        return seo_count

    def attribute_using_seo_csv(self) -> int:
        """Identify SEO traffic using CSV keyword data (current implementation)"""
        # Use the actual column name returned by traffic_loader
        keyword_column = 'keyword' if 'keyword' in self.seo_keywords_df.columns else 'keyphrase'
        position_column = 'position' if 'position' in self.seo_keywords_df.columns else 'current_position'
        
        # Verify keyword column exists before processing
        if keyword_column not in self.seo_keywords_df.columns:
            print_colored(f"ERROR: Neither 'keyword' nor 'keyphrase' column found in SEO data", Colors.RED)
            print_colored(f"Available columns: {list(self.seo_keywords_df.columns)}", Colors.RED)
            return 0
            
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
                try:
                    # Use the actual column name
                    if keyword_column not in seo_kw or pd.isna(seo_kw[keyword_column]):
                        continue
                        
                    seo_keyword = str(seo_kw[keyword_column]).lower()
                    seo_keyword_terms = self.extract_keywords_from_text(seo_keyword)

                    for lead_kw in lead_keywords:
                        for seo_kw_term in seo_keyword_terms:
                            if FUZZY_AVAILABLE:
                                similarity = fuzz.token_sort_ratio(lead_kw, seo_kw_term)
                            else:
                                similarity = 100 if lead_kw == seo_kw_term else 0
                            
                            if similarity > 60:
                                # Higher score for better rankings (check column exists)
                                position = seo_kw[position_column] if position_column in seo_kw and pd.notna(seo_kw[position_column]) else 100
                                position_bonus = max(0, 10 - position) * 3
                                adjusted_score = similarity + position_bonus
                                matched_positions.append(position)
                                
                                keyword_match_score = max(keyword_match_score, adjusted_score)
                                matched_keywords.append((lead_kw, seo_kw_term, similarity))
                                
                except KeyError as e:
                    print_colored(f"Warning: Column error in SEO data: {e}", Colors.YELLOW)
                    continue

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

        # Add null check
        if self.combined_ppc_df is None:
            print_colored("PPC data is None - initializing as empty DataFrame", Colors.YELLOW)
            self.combined_ppc_df = pd.DataFrame()
        
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
            valid_timestamp_leads['inquiry_hour'] = valid_timestamp_leads['first_inquiry_timestamp'].dt.floor('h')
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
                inquiry_hour = inquiry_time.floor('h')
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

    def validate_attribution_with_ga4(self):
        """Validate attributions using GA4 traffic patterns"""
        if not self.use_ga4 or self.ga4_traffic_data is None or self.ga4_traffic_data.empty:
            print_colored("GA4 validation skipped - no traffic data available", Colors.BLUE)
            return
            
        print_colored("Validating attributions with GA4 traffic patterns...", Colors.BLUE)
        
        validated_count = 0
        boosted_count = 0
        
        # Initialize GA4 validation columns
        self.leads_df['ga4_validated'] = False
        self.leads_df['ga4_sessions'] = 0
        
        for idx, lead in self.leads_df.iterrows():
            if lead['attributed_source'] in ['SEO', 'PPC', 'Unknown']:
                # Check for traffic near lead time
                lead_time = pd.to_datetime(lead['first_ticket_date'])
                if pd.isna(lead_time):
                    continue
                
                # Look for traffic within 2 hours
                time_window_start = lead_time - pd.Timedelta(hours=2)
                time_window_end = lead_time + pd.Timedelta(hours=1)
                
                # Find matching traffic
                matching_traffic = self.ga4_traffic_data[
                    (self.ga4_traffic_data['datetime'] >= time_window_start) &
                    (self.ga4_traffic_data['datetime'] <= time_window_end)
                ]
                
                if not matching_traffic.empty:
                    # Check source alignment
                    source_map = {
                        'SEO': ['google', 'bing', 'yahoo'],  # organic sources
                        'PPC': ['google', 'bing', 'facebook']  # paid sources
                    }

                    medium_map = {
                        'SEO': ['organic'],
                        'PPC': ['cpc', 'ppc', 'paid', 'cpm']
                    }
                    
                    current_source = lead['attributed_source']
                    
                    # Find relevant traffic
                    if current_source in source_map:
                        relevant_traffic = matching_traffic[
                            (matching_traffic['source'].str.lower().isin(source_map[current_source])) &
                            (matching_traffic['medium'].str.lower().isin(medium_map.get(current_source, [])))
                        ]
                        
                        if not relevant_traffic.empty:
                            # Boost confidence
                            sessions = relevant_traffic['sessions'].sum()
                            boost_factor = min(1.3, 1 + (sessions / 100))
                            
                            original_confidence = lead['attribution_confidence']
                            new_confidence = min(100, original_confidence * boost_factor)
                            
                            self.leads_df.loc[idx, 'attribution_confidence'] = new_confidence
                            self.leads_df.loc[idx, 'ga4_validated'] = True
                            self.leads_df.loc[idx, 'ga4_sessions'] = sessions
                            
                            # Update attribution detail to include GA4 validation
                            current_detail = lead.get('attribution_detail', '')
                            ga4_detail = f" | GA4: {sessions} sessions validated"
                            self.leads_df.loc[idx, 'attribution_detail'] = current_detail + ga4_detail
                            
                            validated_count += 1
                            if new_confidence > original_confidence:
                                boosted_count += 1
        
        # Special handling for PPC detection using GA4
        print_colored("\nChecking for PPC attribution using GA4 data...", Colors.BLUE)
        ppc_attributed = 0
        
        # Look at all leads (not just Unknown) that might be PPC
        for idx, lead in self.leads_df.iterrows():
            if lead['attributed_source'] in ['Unknown', 'SEO']:  # Check Unknown and SEO (might be misattributed)
                lead_time = pd.to_datetime(lead['first_ticket_date'])
                if pd.isna(lead_time):
                    continue
                
                # Look for PPC traffic within 48 hours before lead
                time_window_start = lead_time - pd.Timedelta(hours=48)
                time_window_end = lead_time + pd.Timedelta(minutes=30)
                
                # Find CPC/PPC traffic in GA4 data
                ppc_traffic = self.ga4_traffic_data[
                    (self.ga4_traffic_data['datetime'] >= time_window_start) &
                    (self.ga4_traffic_data['datetime'] <= time_window_end) &
                    (self.ga4_traffic_data['medium'].isin(['cpc', 'ppc', 'paid']))
                ]
                
                if not ppc_traffic.empty:
                    # Found PPC traffic near lead time
                    sessions = ppc_traffic['sessions'].sum()
                    sources = ppc_traffic['source'].value_counts().head(1)
                    
                    if sessions > 0:
                        # Re-attribute to PPC
                        if lead['attributed_source'] == 'Unknown':
                            self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                            self.leads_df.loc[idx, 'attribution_confidence'] = min(85, 60 + (sessions * 2))
                            self.leads_df.loc[idx, 'attribution_detail'] = f"GA4 PPC detection: {sources.index[0]}/cpc ({sessions} sessions)"
                            self.leads_df.loc[idx, 'data_source'] = 'ga4_ppc'
                            ppc_attributed += 1
                        elif lead['attributed_source'] == 'SEO' and sessions > 5:
                            # Strong PPC signal - might override weak SEO attribution
                            if lead['attribution_confidence'] < 80:
                                self.leads_df.loc[idx, 'attributed_source'] = 'PPC'
                                self.leads_df.loc[idx, 'attribution_confidence'] = min(90, 70 + (sessions * 2))
                                self.leads_df.loc[idx, 'attribution_detail'] = f"GA4 PPC override: {sources.index[0]}/cpc ({sessions} sessions)"
                                self.leads_df.loc[idx, 'data_source'] = 'ga4_ppc'
                                ppc_attributed += 1
        
        print_colored(f"✓ GA4 validation complete: {validated_count} attributions validated", Colors.GREEN)
        print_colored(f"  - {boosted_count} confidence scores boosted", Colors.GREEN)
        if ppc_attributed > 0:
            print_colored(f"  - {ppc_attributed} leads attributed to PPC using GA4 data", Colors.GREEN)

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
        
        # Make sure PPC is included in the final summary even if count is 0
        source_order = ['Direct', 'SEO', 'PPC', 'Referral', 'Unknown']
        for source in source_order:
            count = (self.leads_df['attributed_source'] == source).sum()
            if count > 0 or source == 'PPC':  # Always show PPC
                percentage = count/len(self.leads_df)*100
                print_colored(f"  {source}: {count} leads ({percentage:.1f}%)", Colors.GREEN)
        
        # Show any other sources not in the predefined order
        for source, count in attribution_counts.items():
            if source not in source_order:
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
                              use_gsc=False,
                              gsc_client=None,
                              gsc_credentials_path=None,
                              gsc_property_url=None,
                              use_ga4=False,
                              ga4_property_id=None,
                              compare_methods=False,
                              generate_reports=True):
    """Main function to run traffic attribution analysis"""
    try:
        print_colored("=== Traffic Attribution Analysis ===", Colors.BOLD + Colors.BLUE)
        
        # Initialize analyzer with GSC and GA4 support
        analyzer = LeadAttributionAnalyzer(
            use_gsc=use_gsc,
            gsc_credentials_path=gsc_credentials_path,
            gsc_property_url=gsc_property_url,
            gsc_client=gsc_client,
            use_ga4=use_ga4,
            ga4_property_id=ga4_property_id,
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
