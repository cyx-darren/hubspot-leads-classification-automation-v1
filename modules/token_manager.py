
import os
import json
from replit import db

class TokenManager:
    def __init__(self):
        self.token_key = "quickbooks_refresh_token"
    
    def get_refresh_token(self):
        """Get token from Replit DB first, fall back to Secrets"""
        # Try Replit database first
        if self.token_key in db:
            token = db[self.token_key]
            print(f"Using refresh token from Replit DB (length: {len(token)})")
            return token
        
        # Fall back to environment variable
        token = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')
        if token:
            print(f"Using refresh token from Secrets (length: {len(token)})")
            # Save to DB for future use
            self.save_refresh_token(token)
        return token
    
    def save_refresh_token(self, new_token):
        """Save new refresh token to Replit DB"""
        db[self.token_key] = new_token
        print(f"✓ Saved new refresh token to Replit DB (length: {len(new_token)})")
    
    def clear_stored_token(self):
        """Clear stored token from DB (useful for debugging)"""
        if self.token_key in db:
            del db[self.token_key]
            print("✓ Cleared stored refresh token from Replit DB")
        else:
            print("No stored token found in Replit DB")
