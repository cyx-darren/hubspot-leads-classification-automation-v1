
import pandas as pd
import os

def inspect_leads_data():
    """Inspect the structure of leads data to understand available columns"""
    
    files_to_check = [
        "output/leads_with_products.csv",
        "output/not_spam_leads.csv", 
        "data/leads_feb2025.csv"
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"\n{'='*60}")
            print(f"INSPECTING: {file_path}")
            print(f"{'='*60}")
            
            try:
                df = pd.read_csv(file_path)
                print(f"Rows: {len(df)}")
                print(f"Columns: {len(df.columns)}")
                
                print(f"\nAll columns ({len(df.columns)}):")
                for i, col in enumerate(df.columns, 1):
                    print(f"  {i:2d}. {col}")
                
                # Look for email content related columns
                email_content_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in ['subject', 'content', 'message', 'conversation', 'snippet', 'email', 'ticket'])]
                
                if email_content_columns:
                    print(f"\nEmail/Content related columns:")
                    for col in email_content_columns:
                        print(f"  - {col}")
                        # Show sample data
                        sample_data = df[col].dropna().head(2)
                        if len(sample_data) > 0:
                            for idx, value in sample_data.items():
                                preview = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                                print(f"    Sample: {preview}")
                else:
                    print(f"\nNo email/content related columns found")
                
                # Check for timestamp columns
                timestamp_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in ['date', 'time', 'timestamp', 'created'])]
                
                if timestamp_columns:
                    print(f"\nTimestamp related columns:")
                    for col in timestamp_columns:
                        print(f"  - {col}")
                
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        else:
            print(f"\n❌ File not found: {file_path}")

if __name__ == "__main__":
    inspect_leads_data()
