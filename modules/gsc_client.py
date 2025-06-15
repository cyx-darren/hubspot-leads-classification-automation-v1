
#!/usr/bin/env python
"""
Google Search Console Client Module for HubSpot Automation v1

This module provides real-time SEO click data from Google Search Console API
to enhance traffic attribution accuracy beyond CSV ranking data.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import pandas as pd

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

# Configure logging
logger = logging.getLogger('gsc_client')

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GSC_AVAILABLE = True
except ImportError:
    print_colored("Warning: Google API libraries not installed - GSC client will not function", Colors.YELLOW)
    print_colored("Install with: pip install google-auth google-auth-oauthlib google-api-python-client", Colors.YELLOW)
    GSC_AVAILABLE = False

class GoogleSearchConsoleClient:
    """
    Client for accessing Google Search Console API data.
    Provides real click data to enhance SEO attribution accuracy.
    """
    
    def __init__(self, credentials_path=None, property_url=None):
        """
        Initialize GSC client with service account credentials.
        
        Args:
            credentials_path: Path to service account JSON file (optional, will try environment first)
            property_url: GSC property URL (e.g., 'https://example.com/')
        """
        self.property_url = property_url
        self.service = None
        self.credentials_path = credentials_path
        self.authenticated = False
        
        if not GSC_AVAILABLE:
            print_colored("GSC client initialized but Google API libraries not available", Colors.YELLOW)
            return
            
        # Auto-authenticate if property_url provided or available in environment
        if property_url or os.environ.get('GSC_PROPERTY_URL'):
            try:
                self.authenticate(property_url)
            except Exception as e:
                print_colored(f"Auto-authentication failed: {e}", Colors.YELLOW)
    
    def get_credentials(self):
        """Get credentials from file or environment"""
        # Try environment variable first
        creds_json = os.environ.get('GSC_CREDENTIALS')
        if creds_json:
            try:
                return json.loads(creds_json)
            except json.JSONDecodeError:
                print_colored("Error: Invalid JSON in GSC_CREDENTIALS", Colors.RED)
                return None
        
        # Try file as fallback
        creds_path = self.credentials_path or "data/gsc_credentials.json"
        if os.path.exists(creds_path):
            try:
                with open(creds_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                print_colored(f"Error reading credentials from {creds_path}", Colors.RED)
                return None
        
        return None
    
    def authenticate(self, property_url: str = None) -> bool:
        """
        Authenticate using service account credentials from environment or file.
        
        Args:
            property_url: GSC property URL (e.g., 'https://example.com/')
            
        Returns:
            True if authentication successful, False otherwise
        """
        if not GSC_AVAILABLE:
            print_colored("Cannot authenticate: Google API libraries not available", Colors.RED)
            return False
            
        try:
            # Use provided URL or get from environment
            self.property_url = property_url or os.environ.get('GSC_PROPERTY_URL')
            
            if not self.property_url:
                print_colored("Error: No property URL provided. Set GSC_PROPERTY_URL in environment.", Colors.RED)
                return False
            
            # Get credentials from environment or file
            creds_data = self.get_credentials()
            if not creds_data:
                print_colored("Error: No GSC credentials found in environment or file", Colors.RED)
                return False
            
            # Load service account credentials from data
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    creds_data,
                    scopes=['https://www.googleapis.com/auth/webmasters.readonly']
                )
                print_colored("✓ Loaded service account credentials from environment/file", Colors.GREEN)
            except Exception as e:
                print_colored(f"Error loading credentials: {e}", Colors.RED)
                return False
            
            # Build the Search Console service
            try:
                self.service = build('searchconsole', 'v1', credentials=credentials)
                print_colored("✓ Google Search Console service initialized", Colors.GREEN)
            except Exception as e:
                print_colored(f"Error building GSC service: {e}", Colors.RED)
                return False
            
            # Validate property URL format
            if not property_url.startswith(('http://', 'https://')):
                print_colored(f"Warning: Property URL should start with http:// or https://", Colors.YELLOW)
                property_url = f"https://{property_url}"
            
            if not property_url.endswith('/'):
                property_url += '/'
            
            self.property_url = property_url
            
            # Test authentication by listing sites
            try:
                sites_response = self.service.sites().list().execute()
                available_sites = [site['siteUrl'] for site in sites_response.get('siteEntry', [])]
                
                if self.property_url in available_sites:
                    print_colored(f"✓ Authenticated for property: {self.property_url}", Colors.GREEN)
                    self.authenticated = True
                    return True
                else:
                    print_colored(f"Error: Property {self.property_url} not accessible", Colors.RED)
                    print_colored(f"Available properties: {', '.join(available_sites)}", Colors.BLUE)
                    return False
                    
            except HttpError as e:
                if e.resp.status == 403:
                    print_colored("Error: Access denied - check service account permissions", Colors.RED)
                elif e.resp.status == 429:
                    print_colored("Error: API quota exceeded - try again later", Colors.RED)
                else:
                    print_colored(f"API Error: {e}", Colors.RED)
                return False
            
        except Exception as e:
            print_colored(f"Authentication error: {e}", Colors.RED)
            logger.error(f"GSC authentication error: {e}")
            return False
    
    def get_search_queries(self, start_date: datetime, end_date: datetime, limit: int = 1000) -> Optional[pd.DataFrame]:
        """
        Get search queries with clicks for date range.
        
        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            limit: Maximum number of queries to retrieve
            
        Returns:
            DataFrame with columns: query, clicks, impressions, ctr, position, date
        """
        if not self.authenticated or not self.service:
            print_colored("Error: GSC client not authenticated", Colors.RED)
            return None
            
        try:
            # Format dates for API
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            print_colored(f"Fetching GSC queries from {start_date_str} to {end_date_str}", Colors.BLUE)
            
            # Build request
            request = {
                'startDate': start_date_str,
                'endDate': end_date_str,
                'dimensions': ['query'],
                'rowLimit': limit,
                'startRow': 0
            }
            
            # Execute request with error handling
            try:
                response = self.service.searchanalytics().query(
                    siteUrl=self.property_url,
                    body=request
                ).execute()
                
                print_colored(f"✓ Retrieved {len(response.get('rows', []))} search queries", Colors.GREEN)
                
            except HttpError as e:
                if e.resp.status == 403:
                    print_colored("Error: Insufficient permissions for GSC data", Colors.RED)
                elif e.resp.status == 429:
                    print_colored("Error: GSC API quota exceeded", Colors.RED)
                elif e.resp.status == 400:
                    print_colored(f"Error: Invalid request parameters", Colors.RED)
                else:
                    print_colored(f"GSC API Error: {e}", Colors.RED)
                return None
            
            # Process response
            rows = response.get('rows', [])
            if not rows:
                print_colored("No search query data found for the specified period", Colors.YELLOW)
                return pd.DataFrame(columns=['query', 'clicks', 'impressions', 'ctr', 'position', 'date'])
            
            # Convert to DataFrame
            data = []
            for row in rows:
                data.append({
                    'query': row['keys'][0].lower(),  # Normalize to lowercase
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'] * 100,  # Convert to percentage
                    'position': row['position'],
                    'date': start_date_str  # Use start date as representative date
                })
            
            df = pd.DataFrame(data)
            
            # Add derived metrics for compatibility with existing system
            df['keyword'] = df['query']  # Alias for compatibility
            df['ranking_strength'] = self._calculate_ranking_strength(df['position'])
            df['performance_score'] = self._calculate_gsc_performance_score(
                df['clicks'], df['impressions'], df['ctr']
            )
            
            # Add metadata
            df['source_type'] = 'SEO'
            df['data_source'] = 'gsc_api'
            df['date'] = pd.to_datetime(df['date'])
            
            print_colored(f"✓ Processed GSC data: {len(df)} queries with {df['clicks'].sum()} total clicks", Colors.GREEN)
            
            return df
            
        except Exception as e:
            print_colored(f"Error fetching search queries: {e}", Colors.RED)
            logger.error(f"GSC query error: {e}")
            return None
    
    def get_clicks_by_keywords(self, keywords: List[str], date_range_days: int = 7) -> Optional[pd.DataFrame]:
        """
        Get clicks for specific keywords in recent period.
        
        Args:
            keywords: List of keywords to search for
            date_range_days: Number of days back to search
            
        Returns:
            DataFrame with click data for matching keywords
        """
        if not keywords:
            print_colored("No keywords provided for GSC search", Colors.YELLOW)
            return pd.DataFrame()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range_days)
        
        # Get all queries
        all_queries_df = self.get_search_queries(start_date, end_date, limit=2000)
        
        if all_queries_df is None or all_queries_df.empty:
            return pd.DataFrame()
        
        # Filter for matching keywords
        keywords_lower = [kw.lower() for kw in keywords]
        matched_queries = []
        
        for _, row in all_queries_df.iterrows():
            query = row['query']
            for keyword in keywords_lower:
                if keyword in query or any(term in query for term in keyword.split()):
                    matched_queries.append(row)
                    break
        
        if matched_queries:
            matched_df = pd.DataFrame(matched_queries)
            print_colored(f"✓ Found {len(matched_df)} GSC queries matching provided keywords", Colors.GREEN)
            return matched_df
        else:
            print_colored("No GSC queries found matching provided keywords", Colors.YELLOW)
            return pd.DataFrame()
    
    def get_landing_page_data(self, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Get performance by landing page.
        
        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            
        Returns:
            DataFrame with clicks and queries by page
        """
        if not self.authenticated or not self.service:
            print_colored("Error: GSC client not authenticated", Colors.RED)
            return None
            
        try:
            # Format dates for API
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            print_colored(f"Fetching GSC landing page data from {start_date_str} to {end_date_str}", Colors.BLUE)
            
            # Build request for pages
            request = {
                'startDate': start_date_str,
                'endDate': end_date_str,
                'dimensions': ['page'],
                'rowLimit': 1000,
                'startRow': 0
            }
            
            # Execute request
            try:
                response = self.service.searchanalytics().query(
                    siteUrl=self.property_url,
                    body=request
                ).execute()
                
            except HttpError as e:
                print_colored(f"GSC API Error for landing pages: {e}", Colors.RED)
                return None
            
            # Process response
            rows = response.get('rows', [])
            if not rows:
                print_colored("No landing page data found", Colors.YELLOW)
                return pd.DataFrame(columns=['page', 'clicks', 'impressions', 'ctr', 'position'])
            
            # Convert to DataFrame
            data = []
            for row in rows:
                data.append({
                    'page': row['keys'][0],
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'] * 100,
                    'position': row['position']
                })
            
            df = pd.DataFrame(data)
            print_colored(f"✓ Retrieved landing page data for {len(df)} pages", Colors.GREEN)
            
            return df
            
        except Exception as e:
            print_colored(f"Error fetching landing page data: {e}", Colors.RED)
            logger.error(f"GSC landing page error: {e}")
            return None
    
    def _calculate_ranking_strength(self, positions: pd.Series) -> pd.Series:
        """Calculate ranking strength score based on position (1-100 scale)"""
        import numpy as np
        
        # Higher score for better positions (lower position numbers)
        return np.where(
            positions <= 10,
            100 - (positions - 1) * 5,  # Linear decrease for top 10
            np.where(
                positions <= 50,
                50 - (positions - 10) * 1,  # Slower decrease for 11-50
                10  # Minimum score for 50+
            )
        )
    
    def _calculate_gsc_performance_score(self, clicks: pd.Series, impressions: pd.Series, ctr: pd.Series) -> pd.Series:
        """Calculate performance score based on GSC metrics"""
        import numpy as np
        
        # Weight actual clicks heavily since they represent real traffic
        click_score = np.log1p(clicks) * 15  # Higher weight for actual clicks
        impression_score = np.log1p(impressions) * 5
        ctr_score = ctr * 10  # CTR as percentage * 10
        
        total_score = click_score + impression_score + ctr_score
        
        # Normalize to 0-100 scale
        if total_score.max() > 0:
            return (total_score / total_score.max() * 100).round(2)
        else:
            return pd.Series([0] * len(total_score))
    
    def test_connection(self) -> bool:
        """Test GSC connection and return status"""
        if not self.authenticated:
            return False
            
        try:
            # Simple test query for last 3 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3)
            
            test_df = self.get_search_queries(start_date, end_date, limit=10)
            
            if test_df is not None:
                print_colored(f"✓ GSC connection test successful - retrieved {len(test_df)} queries", Colors.GREEN)
                return True
            else:
                print_colored("GSC connection test failed", Colors.RED)
                return False
                
        except Exception as e:
            print_colored(f"GSC connection test error: {e}", Colors.RED)
            return False
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of available GSC data"""
        if not self.authenticated:
            return {'status': 'not_authenticated', 'available': False}
        
        try:
            # Test with last 7 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            queries_df = self.get_search_queries(start_date, end_date, limit=100)
            
            if queries_df is not None and not queries_df.empty:
                total_clicks = queries_df['clicks'].sum()
                total_impressions = queries_df['impressions'].sum()
                avg_position = queries_df['position'].mean()
                
                return {
                    'status': 'active',
                    'available': True,
                    'property_url': self.property_url,
                    'queries_count': len(queries_df),
                    'total_clicks_7d': total_clicks,
                    'total_impressions_7d': total_impressions,
                    'avg_position_7d': round(avg_position, 1),
                    'top_queries': list(queries_df.nlargest(5, 'clicks')['query']),
                    'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
                }
            else:
                return {
                    'status': 'no_data',
                    'available': True,
                    'property_url': self.property_url,
                    'message': 'No data available for recent period'
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'available': False,
                'error': str(e)
            }

def get_gsc_credentials():
    """Get GSC credentials from Secrets or file"""
    import json
    
    # First try Replit Secrets
    creds_json = os.environ.get('GSC_CREDENTIALS')
    if creds_json:
        try:
            return json.loads(creds_json)
        except json.JSONDecodeError:
            print_colored("Warning: Invalid JSON in GSC_CREDENTIALS secret", Colors.YELLOW)
            return None
    
    # Then try standard file locations
    file_paths = [
        "data/gsc_credentials.json",
        "data/credentials/gsc_credentials.json"
    ]
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                print_colored(f"Warning: Could not read credentials from {file_path}", Colors.YELLOW)
                continue
    
    return None

def get_property_url():
    """Get property URL from environment or config"""
    # Try environment variable first
    url = os.environ.get('GSC_PROPERTY_URL')
    if url:
        return url
    
    # Default to your domain
    return 'https://easyprintsg.com'

def create_gsc_client(credentials_path: str = None, property_url: str = None) -> Optional[GoogleSearchConsoleClient]:
    """
    Factory function to create and authenticate GSC client.
    
    Args:
        credentials_path: Path to service account JSON file (optional)
        property_url: GSC property URL (optional, will use environment)
        
    Returns:
        Authenticated GSC client or None if error
    """
    if not GSC_AVAILABLE:
        print_colored("Cannot create GSC client: Google API libraries not installed", Colors.RED)
        return None
    
    # Check if credentials are available
    credentials = get_gsc_credentials()
    if not credentials:
        print_colored("GSC credentials not found", Colors.YELLOW)
        print_colored("Add GSC_CREDENTIALS to Secrets or upload credentials file", Colors.BLUE)
        return None
    
    # Use provided property URL or get from environment
    if not property_url:
        property_url = get_property_url()
    
    # Create client with credentials path if provided
    client = GoogleSearchConsoleClient(credentials_path=credentials_path)
    
    # Authenticate using the new method
    if client.authenticate(property_url):
        return client
    else:
        return None

# Example usage and testing
if __name__ == "__main__":
    print_colored("=== Google Search Console Client Test ===", Colors.BOLD + Colors.BLUE)
    
    # Test client creation
    client = create_gsc_client()
    
    if client:
        # Test connection
        if client.test_connection():
            print_colored("✓ GSC client ready for use", Colors.GREEN)
            
            # Get summary
            summary = client.get_data_summary()
            print_colored(f"GSC Summary: {summary}", Colors.BLUE)
        else:
            print_colored("GSC client connection failed", Colors.RED)
    else:
        print_colored("GSC client creation failed", Colors.RED)
        print_colored("To use GSC integration:", Colors.BLUE)
        print_colored("1. Set up service account in Google Cloud Console", Colors.BLUE)
        print_colored("2. Add service account to GSC property", Colors.BLUE)
        print_colored("3. Set GSC_CREDENTIALS_PATH and GSC_PROPERTY_URL environment variables", Colors.BLUE)
