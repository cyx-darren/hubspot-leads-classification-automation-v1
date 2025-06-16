
import os
import json
from datetime import datetime

class TokenManager:
    def __init__(self):
        self.token_file = "data/.quickbooks_token.json"
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)
    
    def get_refresh_token(self):
        """Get token from file first, fall back to Secrets"""
        # Try file first
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    token = data.get('refresh_token')
                    saved_at = data.get('saved_at', 'Unknown')
                    print(f"Using refresh token from file (saved: {saved_at}, length: {len(token)})")
                    return token
            except Exception as e:
                print(f"Error reading token file: {e}")
        
        # Fall back to environment variable
        token = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')
        if token:
            print(f"Using refresh token from Secrets (length: {len(token)})")
            # Save to file for future use
            self.save_refresh_token(token)
        return token
    
    def save_refresh_token(self, new_token):
        """Save new refresh token to file"""
        try:
            data = {
                'refresh_token': new_token,
                'saved_at': datetime.now().isoformat(),
                'length': len(new_token)
            }
            with open(self.token_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✓ Saved new refresh token to {self.token_file} (length: {len(new_token)})")
            
            # Set file permissions to be readable only by owner
            os.chmod(self.token_file, 0o600)
        except Exception as e:
            print(f"Error saving token: {e}")
    
    def clear_stored_token(self):
        """Clear stored token from file (useful for debugging)"""
        if os.path.exists(self.token_file):
            try:
                os.remove(self.token_file)
                print("✓ Cleared stored refresh token from file")
            except Exception as e:
                print(f"Error clearing token file: {e}")
        else:
            print("No stored token file found")
    
    def check_token_status(self):
        """Check and display token status"""
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    print(f"Token Status:")
                    print(f"  - Saved at: {data.get('saved_at', 'Unknown')}")
                    print(f"  - Length: {data.get('length', 'Unknown')}")
                    print(f"  - File: {self.token_file}")
                    return True
            except:
                pass
        
        if os.environ.get('QUICKBOOKS_REFRESH_TOKEN'):
            print("Token found in environment variables only")
            return True
        
        print("No QuickBooks token found")
        return False
