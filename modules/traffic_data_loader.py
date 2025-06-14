
#!/usr/bin/env python
"""
Traffic Data Loader for HubSpot Automation v1

This module handles loading and managing data sources for traffic attribution analysis.
Works with the existing pipeline: lead_analyzer.py → traffic_attribution.py
"""

import os
import pandas as pd
import logging
from typing import Optional, Dict, List
from datetime import datetime

# Configure logging
logger = logging.getLogger('traffic_data_loader')

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

class TrafficDataLoader:
    """
    Manages loading of all data sources for traffic attribution analysis.
    Designed to work with the existing pipeline output from lead_analyzer.py
    """
    
    def __init__(self):
        self.enriched_leads_df = None
        self.seo_data_df = None
        self.ppc_standard_df = None
        self.ppc_dynamic_df = None
        self.data_sources_loaded = {
            'enriched_leads': False,
            'seo_data': False,
            'ppc_standard': False,
            'ppc_dynamic': False
        }

    def load_enriched_leads(self, file_path: str = './output/leads_with_products.csv') -> pd.DataFrame:
        """
        Load enriched leads data from lead_analyzer.py output.
        This is the primary data source containing email, products, and ticket analysis.
        
        Expected columns:
        - email
        - original_classification
        - original_reason  
        - total_tickets_analyzed
        - products_mentioned
        - ticket_subjects
        - analysis_period
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Enriched leads file not found: {file_path}")
            
            self.enriched_leads_df = pd.read_csv(file_path)
            
            # Validate expected columns
            required_columns = ['email', 'products_mentioned', 'analysis_period']
            missing_columns = [col for col in required_columns if col not in self.enriched_leads_df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing expected columns: {missing_columns}", Colors.YELLOW)
            
            # Clean and prepare data
            self.enriched_leads_df['email'] = self.enriched_leads_df['email'].astype(str).str.lower().str.strip()
            
            # Fill NaN values
            self.enriched_leads_df['products_mentioned'] = self.enriched_leads_df['products_mentioned'].fillna('')
            self.enriched_leads_df['ticket_subjects'] = self.enriched_leads_df['ticket_subjects'].fillna('')
            
            self.data_sources_loaded['enriched_leads'] = True
            
            print_colored(f"✓ Loaded {len(self.enriched_leads_df)} enriched leads from {file_path}", Colors.GREEN)
            return self.enriched_leads_df
            
        except Exception as e:
            print_colored(f"Error loading enriched leads: {e}", Colors.RED)
            raise

    def load_seo_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load SEO keyword ranking data.
        
        Expected columns:
        - keyphrase/keyword
        - current_position/position/rank
        - search_volume (optional)
        - difficulty (optional)
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"SEO data file not found: {file_path}", Colors.YELLOW)
                return None
            
            # Try different file formats
            if file_path.endswith('.csv'):
                self.seo_data_df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                self.seo_data_df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
            
            # Standardize column names
            column_mapping = {
                'keyphrase': 'keyword',
                'current_position': 'position',
                'rank': 'position'
            }
            
            self.seo_data_df = self.seo_data_df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            if 'keyword' not in self.seo_data_df.columns:
                # Try to find keyword column
                possible_keyword_cols = ['keyphrase', 'query', 'search_term']
                for col in possible_keyword_cols:
                    if col in self.seo_data_df.columns:
                        self.seo_data_df = self.seo_data_df.rename(columns={col: 'keyword'})
                        break
                else:
                    raise ValueError("No keyword column found in SEO data")
            
            if 'position' not in self.seo_data_df.columns:
                print_colored("Warning: No position column found in SEO data", Colors.YELLOW)
                self.seo_data_df['position'] = 50  # Default position
            
            # Clean and validate data
            self.seo_data_df['keyword'] = self.seo_data_df['keyword'].astype(str).str.lower().str.strip()
            self.seo_data_df['position'] = pd.to_numeric(self.seo_data_df['position'], errors='coerce').fillna(100)
            
            # Remove empty keywords
            self.seo_data_df = self.seo_data_df[self.seo_data_df['keyword'].str.len() > 0]
            
            self.data_sources_loaded['seo_data'] = True
            
            print_colored(f"✓ Loaded {len(self.seo_data_df)} SEO keywords from {file_path}", Colors.GREEN)
            return self.seo_data_df
            
        except Exception as e:
            print_colored(f"Error loading SEO data: {e}", Colors.RED)
            return None

    def load_ppc_standard_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load standard PPC campaign data.
        
        Expected columns:
        - date
        - keyword/search_term
        - clicks
        - impressions
        - cost (optional)
        - campaign_name (optional)
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"PPC standard data file not found: {file_path}", Colors.YELLOW)
                return None
            
            # Load data
            if file_path.endswith('.csv'):
                self.ppc_standard_df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                self.ppc_standard_df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
            
            # Standardize column names
            column_mapping = {
                'search_term': 'keyword',
                'query': 'keyword'
            }
            
            self.ppc_standard_df = self.ppc_standard_df.rename(columns=column_mapping)
            
            # Validate required columns
            required_columns = ['date', 'keyword', 'clicks', 'impressions']
            missing_columns = [col for col in required_columns if col not in self.ppc_standard_df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing PPC columns: {missing_columns}", Colors.YELLOW)
                # Try to create missing columns with defaults
                if 'clicks' not in self.ppc_standard_df.columns:
                    self.ppc_standard_df['clicks'] = 1
                if 'impressions' not in self.ppc_standard_df.columns:
                    self.ppc_standard_df['impressions'] = 10
            
            # Clean and process data
            self.ppc_standard_df['keyword'] = self.ppc_standard_df['keyword'].astype(str).str.lower().str.strip()
            self.ppc_standard_df['date'] = pd.to_datetime(self.ppc_standard_df['date'], errors='coerce')
            self.ppc_standard_df['clicks'] = pd.to_numeric(self.ppc_standard_df['clicks'], errors='coerce').fillna(0)
            self.ppc_standard_df['impressions'] = pd.to_numeric(self.ppc_standard_df['impressions'], errors='coerce').fillna(0)
            
            # Add campaign type
            self.ppc_standard_df['campaign_type'] = 'Standard'
            
            # Remove invalid data
            self.ppc_standard_df = self.ppc_standard_df.dropna(subset=['date'])
            self.ppc_standard_df = self.ppc_standard_df[self.ppc_standard_df['keyword'].str.len() > 0]
            
            self.data_sources_loaded['ppc_standard'] = True
            
            print_colored(f"✓ Loaded {len(self.ppc_standard_df)} PPC standard records from {file_path}", Colors.GREEN)
            return self.ppc_standard_df
            
        except Exception as e:
            print_colored(f"Error loading PPC standard data: {e}", Colors.RED)
            return None

    def load_ppc_dynamic_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load dynamic PPC campaign data (Dynamic Search Ads).
        
        Expected columns:
        - date
        - dynamic_ad_target/landing_page
        - clicks
        - impressions
        - cost (optional)
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"PPC dynamic data file not found: {file_path}", Colors.YELLOW)
                return None
            
            # Load data
            if file_path.endswith('.csv'):
                self.ppc_dynamic_df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                self.ppc_dynamic_df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
            
            # Standardize column names for dynamic ads
            column_mapping = {
                'dynamic_ad_target': 'keyword',
                'landing_page': 'keyword',
                'final_url': 'keyword'
            }
            
            self.ppc_dynamic_df = self.ppc_dynamic_df.rename(columns=column_mapping)
            
            # If no keyword column found, try to extract from other columns
            if 'keyword' not in self.ppc_dynamic_df.columns:
                # For dynamic ads, we might need to create synthetic keywords
                if 'campaign_name' in self.ppc_dynamic_df.columns:
                    self.ppc_dynamic_df['keyword'] = self.ppc_dynamic_df['campaign_name'].astype(str).str.lower()
                else:
                    self.ppc_dynamic_df['keyword'] = 'dynamic_ad'
            
            # Validate required columns
            required_columns = ['date', 'clicks', 'impressions']
            missing_columns = [col for col in required_columns if col not in self.ppc_dynamic_df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing PPC dynamic columns: {missing_columns}", Colors.YELLOW)
                # Try to create missing columns with defaults
                if 'clicks' not in self.ppc_dynamic_df.columns:
                    self.ppc_dynamic_df['clicks'] = 1
                if 'impressions' not in self.ppc_dynamic_df.columns:
                    self.ppc_dynamic_df['impressions'] = 10
            
            # Clean and process data
            self.ppc_dynamic_df['keyword'] = self.ppc_dynamic_df['keyword'].astype(str).str.lower().str.strip()
            self.ppc_dynamic_df['date'] = pd.to_datetime(self.ppc_dynamic_df['date'], errors='coerce')
            self.ppc_dynamic_df['clicks'] = pd.to_numeric(self.ppc_dynamic_df['clicks'], errors='coerce').fillna(0)
            self.ppc_dynamic_df['impressions'] = pd.to_numeric(self.ppc_dynamic_df['impressions'], errors='coerce').fillna(0)
            
            # Add campaign type
            self.ppc_dynamic_df['campaign_type'] = 'Dynamic'
            
            # Remove invalid data
            self.ppc_dynamic_df = self.ppc_dynamic_df.dropna(subset=['date'])
            
            self.data_sources_loaded['ppc_dynamic'] = True
            
            print_colored(f"✓ Loaded {len(self.ppc_dynamic_df)} PPC dynamic records from {file_path}", Colors.GREEN)
            return self.ppc_dynamic_df
            
        except Exception as e:
            print_colored(f"Error loading PPC dynamic data: {e}", Colors.RED)
            return None

    def load_all_data(self, 
                     enriched_leads_path: str = './output/leads_with_products.csv',
                     seo_data_path: Optional[str] = None,
                     ppc_standard_path: Optional[str] = None,
                     ppc_dynamic_path: Optional[str] = None) -> Dict[str, bool]:
        """
        Load all available data sources.
        
        Returns:
            Dict indicating which data sources were successfully loaded
        """
        print_colored("Loading all traffic attribution data sources...", Colors.BLUE)
        
        # Load enriched leads (required)
        self.load_enriched_leads(enriched_leads_path)
        
        # Load optional data sources
        if seo_data_path:
            self.load_seo_data(seo_data_path)
        
        if ppc_standard_path:
            self.load_ppc_standard_data(ppc_standard_path)
        
        if ppc_dynamic_path:
            self.load_ppc_dynamic_data(ppc_dynamic_path)
        
        # Summary
        loaded_count = sum(self.data_sources_loaded.values())
        total_sources = len(self.data_sources_loaded)
        
        print_colored(f"\n✓ Data loading complete: {loaded_count}/{total_sources} sources loaded", Colors.GREEN)
        for source, loaded in self.data_sources_loaded.items():
            status = "✓" if loaded else "✗"
            color = Colors.GREEN if loaded else Colors.YELLOW
            print_colored(f"  {status} {source.replace('_', ' ').title()}", color)
        
        return self.data_sources_loaded

    def get_data_summary(self) -> Dict[str, any]:
        """Get summary of loaded data"""
        summary = {
            'enriched_leads_count': len(self.enriched_leads_df) if self.enriched_leads_df is not None else 0,
            'seo_keywords_count': len(self.seo_data_df) if self.seo_data_df is not None else 0,
            'ppc_standard_count': len(self.ppc_standard_df) if self.ppc_standard_df is not None else 0,
            'ppc_dynamic_count': len(self.ppc_dynamic_df) if self.ppc_dynamic_df is not None else 0,
            'data_sources_loaded': self.data_sources_loaded.copy()
        }
        
        return summary

def test_data_loader():
    """Test function for the TrafficDataLoader"""
    print_colored("=== Testing TrafficDataLoader ===", Colors.BOLD + Colors.BLUE)
    
    loader = TrafficDataLoader()
    
    # Test with existing data
    try:
        # Load enriched leads
        enriched_leads = loader.load_enriched_leads()
        print(f"Enriched leads columns: {list(enriched_leads.columns)}")
        print(f"Sample products: {enriched_leads['products_mentioned'].head(3).tolist()}")
        
        # Try to load SEO data if available
        seo_files = ['./data/Feb2025-SEO.csv']
        for seo_file in seo_files:
            if os.path.exists(seo_file):
                loader.load_seo_data(seo_file)
                break
        
        # Try to load PPC data if available
        ppc_files = [
            './data/When your ads showed Custom and Corporate Gifts and Lanyards (1).csv',
            './data/When your ads showed Dynamic Search Ads (1).csv'
        ]
        for ppc_file in ppc_files:
            if os.path.exists(ppc_file):
                if 'Dynamic' in ppc_file:
                    loader.load_ppc_dynamic_data(ppc_file)
                else:
                    loader.load_ppc_standard_data(ppc_file)
        
        # Print summary
        summary = loader.get_data_summary()
        print_colored("\nData Summary:", Colors.BLUE)
        for key, value in summary.items():
            print(f"  {key}: {value}")
        
    except Exception as e:
        print_colored(f"Test failed: {e}", Colors.RED)

if __name__ == "__main__":
    test_data_loader()
