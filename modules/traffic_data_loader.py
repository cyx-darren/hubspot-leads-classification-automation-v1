
#!/usr/bin/env python
"""
Traffic Data Loader Module for HubSpot Automation v1

This module handles loading and standardizing SEO and PPC data from various CSV formats.
Provides consistent data structures for the traffic attribution analysis.
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import warnings

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
    Handles loading and standardizing traffic data from various sources:
    - SEO keyword ranking data
    - PPC standard campaign data
    - PPC dynamic search ads data
    """
    
    def __init__(self):
        self.seo_data = None
        self.ppc_standard_data = None
        self.ppc_dynamic_data = None
        
    def load_seo_keywords(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load SEO keyword data from CSV file.
        Expected format: Keyphrase, Current Page, Current Position
        
        Args:
            file_path: Path to the SEO CSV file
            
        Returns:
            Standardized DataFrame with SEO data or None if error
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"Warning: SEO file not found: {file_path}", Colors.YELLOW)
                return None
                
            # Load the CSV
            df = pd.read_csv(file_path)
            print_colored(f"✓ Loaded SEO data from {file_path}", Colors.GREEN)
            
            # Validate required columns
            required_columns = ['Keyphrase', 'Current Page', 'Current Position']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing SEO columns: {missing_columns}", Colors.YELLOW)
                return None
            
            # Standardize column names
            standardized_df = df.copy()
            standardized_df = standardized_df.rename(columns={
                'Keyphrase': 'keyword',
                'Current Page': 'page',
                'Current Position': 'position'
            })
            
            # Clean and validate data
            standardized_df['keyword'] = standardized_df['keyword'].astype(str).str.strip().str.lower()
            standardized_df['page'] = pd.to_numeric(standardized_df['page'], errors='coerce')
            standardized_df['position'] = pd.to_numeric(standardized_df['position'], errors='coerce')
            
            # Filter out invalid positions (0 means not ranking)
            valid_positions = standardized_df['position'] > 0
            standardized_df = standardized_df[valid_positions].copy()
            
            # Add derived metrics
            standardized_df['ranking_strength'] = self._calculate_ranking_strength(
                standardized_df['position']
            )
            
            # Extract product categories from keywords
            standardized_df['product_category'] = standardized_df['keyword'].apply(
                self._extract_product_category
            )
            
            # Add source type
            standardized_df['source_type'] = 'SEO'
            standardized_df['data_source'] = 'organic_search'
            
            print_colored(f"✓ Processed {len(standardized_df)} SEO keywords", Colors.GREEN)
            
            self.seo_data = standardized_df
            return standardized_df
            
        except Exception as e:
            print_colored(f"Error loading SEO data: {e}", Colors.RED)
            logger.error(f"SEO data loading error: {e}")
            return None
    
    def load_ppc_standard(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load PPC standard campaign data.
        Expected format: Keyword, Clicks, Impr. (Impressions)
        
        Args:
            file_path: Path to the PPC standard CSV file
            
        Returns:
            Standardized DataFrame with PPC data or None if error
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"Warning: PPC standard file not found: {file_path}", Colors.YELLOW)
                return None
                
            # Load the CSV
            df = pd.read_csv(file_path)
            print_colored(f"✓ Loaded PPC standard data from {file_path}", Colors.GREEN)
            
            # Validate required columns
            required_columns = ['Keyword', 'Clicks', 'Impr.']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing PPC standard columns: {missing_columns}", Colors.YELLOW)
                return None
            
            # Standardize column names
            standardized_df = df.copy()
            standardized_df = standardized_df.rename(columns={
                'Keyword': 'keyword',
                'Clicks': 'clicks',
                'Impr.': 'impressions'
            })
            
            # Clean and validate data
            standardized_df['keyword'] = standardized_df['keyword'].astype(str).str.strip().str.lower()
            standardized_df['clicks'] = pd.to_numeric(standardized_df['clicks'], errors='coerce').fillna(0)
            standardized_df['impressions'] = pd.to_numeric(standardized_df['impressions'], errors='coerce').fillna(0)
            
            # Calculate derived metrics
            standardized_df['ctr'] = np.where(
                standardized_df['impressions'] > 0,
                standardized_df['clicks'] / standardized_df['impressions'] * 100,
                0
            )
            
            # Add performance scoring
            standardized_df['performance_score'] = self._calculate_ppc_performance_score(
                standardized_df['clicks'], 
                standardized_df['impressions'],
                standardized_df['ctr']
            )
            
            # Extract product categories from keywords
            standardized_df['product_category'] = standardized_df['keyword'].apply(
                self._extract_product_category
            )
            
            # Add metadata
            standardized_df['source_type'] = 'PPC'
            standardized_df['campaign_type'] = 'Standard'
            standardized_df['data_source'] = 'google_ads'
            
            # Add estimated date (current date as placeholder)
            standardized_df['date'] = datetime.now().strftime('%Y-%m-%d')
            
            print_colored(f"✓ Processed {len(standardized_df)} PPC standard keywords", Colors.GREEN)
            
            self.ppc_standard_data = standardized_df
            return standardized_df
            
        except Exception as e:
            print_colored(f"Error loading PPC standard data: {e}", Colors.RED)
            logger.error(f"PPC standard data loading error: {e}")
            return None
    
    def load_ppc_dynamic(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Load PPC dynamic search ads data.
        Expected format: Dynamic ad target, Clicks, Impr.
        
        Args:
            file_path: Path to the PPC dynamic CSV file
            
        Returns:
            Standardized DataFrame with dynamic PPC data or None if error
        """
        try:
            if not os.path.exists(file_path):
                print_colored(f"Warning: PPC dynamic file not found: {file_path}", Colors.YELLOW)
                return None
                
            # Load the CSV
            df = pd.read_csv(file_path)
            print_colored(f"✓ Loaded PPC dynamic data from {file_path}", Colors.GREEN)
            
            # Validate required columns
            required_columns = ['Dynamic ad target', 'Clicks', 'Impr.']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print_colored(f"Warning: Missing PPC dynamic columns: {missing_columns}", Colors.YELLOW)
                return None
            
            # Standardize column names
            standardized_df = df.copy()
            standardized_df = standardized_df.rename(columns={
                'Dynamic ad target': 'keyword',
                'Clicks': 'clicks',
                'Impr.': 'impressions'
            })
            
            # Clean and validate data
            standardized_df['keyword'] = standardized_df['keyword'].astype(str).str.strip().str.lower()
            standardized_df['clicks'] = pd.to_numeric(standardized_df['clicks'], errors='coerce').fillna(0)
            standardized_df['impressions'] = pd.to_numeric(standardized_df['impressions'], errors='coerce').fillna(0)
            
            # Calculate derived metrics
            standardized_df['ctr'] = np.where(
                standardized_df['impressions'] > 0,
                standardized_df['clicks'] / standardized_df['impressions'] * 100,
                0
            )
            
            # Add performance scoring
            standardized_df['performance_score'] = self._calculate_ppc_performance_score(
                standardized_df['clicks'], 
                standardized_df['impressions'],
                standardized_df['ctr']
            )
            
            # Extract product categories from dynamic targets
            standardized_df['product_category'] = standardized_df['keyword'].apply(
                self._extract_product_category_from_dynamic_target
            )
            
            # Add metadata
            standardized_df['source_type'] = 'PPC'
            standardized_df['campaign_type'] = 'Dynamic'
            standardized_df['data_source'] = 'google_ads'
            
            # Add estimated date (current date as placeholder)
            standardized_df['date'] = datetime.now().strftime('%Y-%m-%d')
            
            print_colored(f"✓ Processed {len(standardized_df)} PPC dynamic targets", Colors.GREEN)
            
            self.ppc_dynamic_data = standardized_df
            return standardized_df
            
        except Exception as e:
            print_colored(f"Error loading PPC dynamic data: {e}", Colors.RED)
            logger.error(f"PPC dynamic data loading error: {e}")
            return None
    
    def standardize_dates(self, date_column: pd.Series, date_format: str = None) -> pd.Series:
        """
        Convert various date formats to standardized datetime objects.
        
        Args:
            date_column: Series containing date strings
            date_format: Optional specific format to try first
            
        Returns:
            Series with standardized datetime objects
        """
        try:
            # Common date formats to try
            formats_to_try = [
                '%d/%m/%y',      # DD/MM/YY
                '%d/%m/%Y',      # DD/MM/YYYY
                '%m/%d/%y',      # MM/DD/YY
                '%m/%d/%Y',      # MM/DD/YYYY
                '%Y-%m-%d',      # YYYY-MM-DD
                '%d-%m-%Y',      # DD-MM-YYYY
            ]
            
            # If specific format provided, try it first
            if date_format:
                formats_to_try.insert(0, date_format)
            
            standardized_dates = pd.Series(index=date_column.index, dtype='datetime64[ns]')
            
            for fmt in formats_to_try:
                try:
                    # Try to parse remaining null dates with this format
                    mask = standardized_dates.isna()
                    if mask.any():
                        parsed = pd.to_datetime(date_column[mask], format=fmt, errors='coerce')
                        standardized_dates[mask] = parsed
                except:
                    continue
            
            # Final attempt with pandas automatic parsing
            mask = standardized_dates.isna()
            if mask.any():
                standardized_dates[mask] = pd.to_datetime(date_column[mask], errors='coerce')
            
            success_rate = (1 - standardized_dates.isna().sum() / len(standardized_dates)) * 100
            print_colored(f"✓ Date standardization: {success_rate:.1f}% success rate", Colors.GREEN)
            
            return standardized_dates
            
        except Exception as e:
            print_colored(f"Error standardizing dates: {e}", Colors.RED)
            logger.error(f"Date standardization error: {e}")
            return date_column
    
    def _calculate_ranking_strength(self, positions: pd.Series) -> pd.Series:
        """Calculate ranking strength score based on position (1-100 scale)"""
        # Higher score for better positions (lower position numbers)
        # Position 1 = 100 points, Position 10 = 55 points, Position 50+ = 10 points
        return np.where(
            positions <= 10,
            100 - (positions - 1) * 5,  # Linear decrease for top 10
            np.where(
                positions <= 50,
                50 - (positions - 10) * 1,  # Slower decrease for 11-50
                10  # Minimum score for 50+
            )
        )
    
    def _calculate_ppc_performance_score(self, clicks: pd.Series, impressions: pd.Series, ctr: pd.Series) -> pd.Series:
        """Calculate PPC performance score based on clicks, impressions, and CTR"""
        # Normalize each metric and combine
        click_score = np.log1p(clicks) * 10  # Log scale for clicks
        impression_score = np.log1p(impressions) * 5  # Log scale for impressions
        ctr_score = ctr * 20  # CTR as percentage * 20
        
        total_score = click_score + impression_score + ctr_score
        
        # Normalize to 0-100 scale
        if total_score.max() > 0:
            return (total_score / total_score.max() * 100).round(2)
        else:
            return pd.Series([0] * len(total_score))
    
    def _extract_product_category(self, keyword: str) -> str:
        """Extract product category from keyword"""
        if not isinstance(keyword, str):
            return 'unknown'
        
        keyword_lower = keyword.lower()
        
        # Product category mapping
        category_keywords = {
            'bags': ['bag', 'tote', 'jute', 'shopping bag', 'paper bag'],
            'apparel': ['shirt', 'polo', 'jacket', 'vest', 'singlet', 'tee'],
            'stationery': ['notebook', 'pen', 'pencil', 'stationery', 'letterhead'],
            'promotional': ['lanyard', 'badge', 'wristband', 'keychain', 'coaster'],
            'drinkware': ['mug', 'tumbler', 'bottle', 'cup', 'flask'],
            'tech': ['usb', 'charger', 'bluetooth', 'adapter', 'fan'],
            'gifts': ['gift', 'corporate gift', 'promotional item'],
            'printing': ['printing', 'print', 'custom', 'personalized'],
            'safety': ['safety vest', 'hi vis', 'reflective']
        }
        
        for category, keywords in category_keywords.items():
            if any(kw in keyword_lower for kw in keywords):
                return category
        
        return 'general'
    
    def _extract_product_category_from_dynamic_target(self, target: str) -> str:
        """Extract product category from dynamic ad target"""
        if not isinstance(target, str):
            return 'unknown'
        
        target_lower = target.lower()
        
        # Extract category from "Category equals X" format
        if 'category equals' in target_lower:
            category_part = target_lower.replace('category equals', '').strip()
            
            # Map specific categories
            category_mapping = {
                'corporate gifts': 'gifts',
                'corporate gifts/pens': 'stationery',
                'shirts': 'apparel',
                'gildan': 'apparel',
                'bags/jute bags': 'bags',
                'bags/tote bags': 'bags',
                'bags/dry bags': 'bags',
                'notebooks/eco friendly note book': 'stationery',
                'notepads': 'stationery'
            }
            
            return category_mapping.get(category_part, 'general')
        
        # Fallback to regular keyword extraction
        return self._extract_product_category(target)
    
    def load_all_data(self, seo_path: str = None, ppc_standard_path: str = None, ppc_dynamic_path: str = None) -> Dict[str, Any]:
        """
        Load all traffic data sources at once.
        
        Args:
            seo_path: Path to SEO CSV file
            ppc_standard_path: Path to PPC standard CSV file
            ppc_dynamic_path: Path to PPC dynamic CSV file
            
        Returns:
            Dictionary with loaded DataFrames and summary stats
        """
        print_colored("Loading all traffic data sources...", Colors.BLUE)
        
        results = {
            'seo_data': None,
            'ppc_standard_data': None,
            'ppc_dynamic_data': None,
            'summary': {}
        }
        
        # Load SEO data
        if seo_path:
            results['seo_data'] = self.load_seo_keywords(seo_path)
        
        # Load PPC standard data
        if ppc_standard_path:
            results['ppc_standard_data'] = self.load_ppc_standard(ppc_standard_path)
        
        # Load PPC dynamic data
        if ppc_dynamic_path:
            results['ppc_dynamic_data'] = self.load_ppc_dynamic(ppc_dynamic_path)
        
        # Create summary
        results['summary'] = {
            'seo_keywords': len(results['seo_data']) if results['seo_data'] is not None else 0,
            'ppc_standard_keywords': len(results['ppc_standard_data']) if results['ppc_standard_data'] is not None else 0,
            'ppc_dynamic_targets': len(results['ppc_dynamic_data']) if results['ppc_dynamic_data'] is not None else 0,
            'total_traffic_data_points': sum([
                len(df) for df in [results['seo_data'], results['ppc_standard_data'], results['ppc_dynamic_data']] 
                if df is not None
            ])
        }
        
        print_colored(f"✓ Traffic data loading complete: {results['summary']['total_traffic_data_points']} total data points", Colors.GREEN)
        
        return results
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of loaded data"""
        summary = {
            'seo': {
                'loaded': self.seo_data is not None,
                'records': len(self.seo_data) if self.seo_data is not None else 0,
                'top_keywords': list(self.seo_data.head(5)['keyword']) if self.seo_data is not None else []
            },
            'ppc_standard': {
                'loaded': self.ppc_standard_data is not None,
                'records': len(self.ppc_standard_data) if self.ppc_standard_data is not None else 0,
                'top_performers': list(self.ppc_standard_data.nlargest(5, 'clicks')['keyword']) if self.ppc_standard_data is not None else []
            },
            'ppc_dynamic': {
                'loaded': self.ppc_dynamic_data is not None,
                'records': len(self.ppc_dynamic_data) if self.ppc_dynamic_data is not None else 0,
                'top_categories': list(self.ppc_dynamic_data['product_category'].value_counts().head(3).index) if self.ppc_dynamic_data is not None else []
            }
        }
        
        return summary
