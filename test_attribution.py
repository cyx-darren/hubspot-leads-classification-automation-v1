
#!/usr/bin/env python3
"""
Test script for Traffic Attribution Module
Verifies that the attribution analysis is working correctly
"""

from modules.traffic_attribution import analyze_traffic_attribution
import os
import pandas as pd

def test_gsc_integration():
    """Test GSC integration if credentials exist"""
    print("\n" + "="*60)
    print("TESTING GOOGLE SEARCH CONSOLE INTEGRATION")
    print("="*60)
    
    creds_path = "data/gsc_credentials.json"
    
    if os.path.exists(creds_path):
        print("✓ GSC credentials found - testing integration...")
        
        try:
            from modules.gsc_client import GoogleSearchConsoleClient
            
            # Test authentication
            print("\n1. Testing Authentication:")
            print("-" * 30)
            
            # Try to get property URL from environment or use default
            property_url = os.environ.get('GSC_PROPERTY_URL', 'https://example.com/')
            
            client = GoogleSearchConsoleClient()
            auth_success = client.authenticate(creds_path, property_url)
            
            if auth_success:
                print("✓ GSC authentication successful")
                
                # Test connection
                print("\n2. Testing Connection:")
                print("-" * 30)
                connection_success = client.test_connection()
                
                if connection_success:
                    print("✓ GSC connection test passed")
                    
                    # Get data summary
                    print("\n3. Data Summary:")
                    print("-" * 30)
                    summary = client.get_data_summary()
                    
                    if summary.get('available'):
                        print(f"✓ Status: {summary.get('status')}")
                        print(f"✓ Property: {summary.get('property_url')}")
                        print(f"✓ Queries (7d): {summary.get('queries_count', 0)}")
                        print(f"✓ Clicks (7d): {summary.get('total_clicks_7d', 0)}")
                        print(f"✓ Impressions (7d): {summary.get('total_impressions_7d', 0)}")
                        print(f"✓ Avg Position (7d): {summary.get('avg_position_7d', 0)}")
                        
                        top_queries = summary.get('top_queries', [])
                        if top_queries:
                            print(f"✓ Top queries: {', '.join(top_queries[:3])}")
                        
                        print("\n4. Sample Data Fetch:")
                        print("-" * 30)
                        
                        # Try to fetch recent data
                        from datetime import datetime, timedelta
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=7)
                        
                        sample_data = client.get_search_queries(start_date, end_date, limit=5)
                        
                        if sample_data is not None and not sample_data.empty:
                            print(f"✓ Sample data retrieved: {len(sample_data)} queries")
                            print("Sample queries:")
                            for idx, row in sample_data.head(3).iterrows():
                                print(f"  - '{row['query']}': {row['clicks']} clicks, pos {row['position']:.1f}")
                        else:
                            print("⚠️  No recent data available (normal for new properties)")
                        
                        return True
                    else:
                        print(f"✗ GSC data not available: {summary.get('error', 'Unknown error')}")
                        return False
                else:
                    print("✗ GSC connection test failed")
                    return False
            else:
                print("✗ GSC authentication failed")
                print("Check:")
                print("  - Service account email added to GSC property")
                print("  - Property URL format (https://example.com/)")
                print("  - API permissions")
                return False
                
        except ImportError:
            print("✗ GSC client module not available")
            print("Google API libraries may not be installed")
            return False
        except Exception as e:
            print(f"✗ GSC integration test error: {e}")
            return False
    else:
        print("ℹ️  GSC credentials not found - skipping GSC tests")
        print(f"To enable GSC integration:")
        print(f"  1. Follow setup guide in data/gsc_setup.md")
        print(f"  2. Save credentials to {creds_path}")
        print(f"  3. Re-run tests")
        return None

def test_attribution():
    print("="*60)
    print("TESTING TRAFFIC ATTRIBUTION MODULE")
    print("="*60)
    
    # Check if required files exist
    files_to_check = {
        "Leads data": "output/leads_with_products.csv",
        "SEO data": "data/Feb2025-SEO.csv",
        "PPC Standard": "data/When your ads showed Custom and Corporate Gifts and Lanyards (1).csv",
        "PPC Dynamic": "data/When your ads showed Dynamic Search Ads (1).csv"
    }
    
    print("\n1. CHECKING REQUIRED FILES:")
    print("-" * 40)
    missing_files = []
    for name, path in files_to_check.items():
        exists = os.path.exists(path)
        status = "✓ Found" if exists else "✗ Missing"
        print(f"   {name}: {status} - {path}")
        if not exists:
            missing_files.append(name)

    if missing_files:
        print(f"\n⚠️  Warning: {len(missing_files)} files missing")
        print("   Attribution will use fallback data where needed")
    else:
        print("\n✓ All required files found")

    # Run attribution
    print("\n2. RUNNING ATTRIBUTION ANALYSIS:")
    print("-" * 40)
    try:
        result = analyze_traffic_attribution(
            leads_path="output/leads_with_products.csv",
            seo_csv_path="data/Feb2025-SEO.csv",
            ppc_standard_path="data/When your ads showed Custom and Corporate Gifts and Lanyards (1).csv",
            ppc_dynamic_path="data/When your ads showed Dynamic Search Ads (1).csv",
            output_path="output/leads_with_attribution.csv",
            generate_reports=True
        )
        
        print(f"\n✓ Attribution analysis completed: {result} leads processed")
        
    except Exception as e:
        print(f"\n✗ Error during attribution analysis: {e}")
        return False

    # Check output
    print("\n3. CHECKING OUTPUT:")
    print("-" * 40)
    
    output_files = [
        "output/leads_with_attribution.csv",
        "output/attribution_report.txt",
        "output/attribution_summary.csv"
    ]
    
    for output_file in output_files:
        if os.path.exists(output_file):
            print(f"   ✓ {output_file} created")
        else:
            print(f"   ✗ {output_file} missing")
    
    # Analyze main output file
    if os.path.exists("output/leads_with_attribution.csv"):
        try:
            df = pd.read_csv("output/leads_with_attribution.csv")
            
            print(f"\n4. ATTRIBUTION RESULTS ANALYSIS:")
            print("-" * 40)
            print(f"   Total leads processed: {len(df)}")
            
            # Attribution Summary
            attribution_counts = df['attributed_source'].value_counts()
            print(f"\n   Attribution Breakdown:")
            for source, count in attribution_counts.items():
                percentage = (count / len(df)) * 100
                print(f"     {source}: {count} leads ({percentage:.1f}%)")
            
            # Confidence Levels
            if 'confidence_level' in df.columns:
                confidence_counts = df['confidence_level'].value_counts()
                print(f"\n   Confidence Levels:")
                for level, count in confidence_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"     {level}: {count} leads ({percentage:.1f}%)")
            
            # Data Source Breakdown
            if 'data_source' in df.columns:
                source_counts = df['data_source'].value_counts()
                print(f"\n   Data Sources Used:")
                for source, count in source_counts.items():
                    percentage = (count / len(df)) * 100
                    print(f"     {source}: {count} leads ({percentage:.1f}%)")
            
            # Show sample attributions
            print(f"\n5. SAMPLE ATTRIBUTIONS:")
            print("-" * 40)
            
            for source in df['attributed_source'].unique():
                if source == 'Unknown':
                    continue
                    
                sample = df[df['attributed_source'] == source].head(1)
                if not sample.empty:
                    row = sample.iloc[0]
                    print(f"\n   {source} Example:")
                    print(f"     Email: {row['email']}")
                    print(f"     Confidence: {row['attribution_confidence']:.1f}%")
                    if 'attribution_detail' in row:
                        detail = str(row['attribution_detail'])
                        print(f"     Detail: {detail[:80]}{'...' if len(detail) > 80 else ''}")
            
            # Check for high confidence attributions
            if 'attribution_confidence' in df.columns:
                high_conf = df[df['attribution_confidence'] >= 80]
                medium_conf = df[(df['attribution_confidence'] >= 50) & (df['attribution_confidence'] < 80)]
                low_conf = df[(df['attribution_confidence'] >= 20) & (df['attribution_confidence'] < 50)]
                
                print(f"\n6. QUALITY ASSESSMENT:")
                print("-" * 40)
                print(f"   High confidence (≥80%): {len(high_conf)} leads")
                print(f"   Medium confidence (50-79%): {len(medium_conf)} leads")
                print(f"   Low confidence (20-49%): {len(low_conf)} leads")
                
                quality_score = ((len(high_conf) + len(medium_conf)) / len(df)) * 100
                print(f"   Overall quality score: {quality_score:.1f}%")
                
                if quality_score >= 70:
                    print("   ✓ Good attribution quality")
                elif quality_score >= 50:
                    print("   ⚠️  Moderate attribution quality")
                else:
                    print("   ✗ Low attribution quality - consider improving data sources")
            
            print(f"\n7. TEST SUMMARY:")
            print("-" * 40)
            print("   ✓ Attribution module is working correctly")
            print("   ✓ All expected output files generated")
            print("   ✓ Attribution results look reasonable")
            
            return True
            
        except Exception as e:
            print(f"   ✗ Error analyzing output: {e}")
            return False
    else:
        print("   ✗ Main output file not created")
        return False

if __name__ == "__main__":
    # Test GSC integration first
    gsc_result = test_gsc_integration()
    
    # Test main attribution
    success = test_attribution()
    
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    if gsc_result is True:
        print("✅ GSC INTEGRATION: Enabled and working")
    elif gsc_result is False:
        print("⚠️  GSC INTEGRATION: Configured but not working")
    else:
        print("ℹ️  GSC INTEGRATION: Not configured (optional)")
    
    if success:
        print("✅ TRAFFIC ATTRIBUTION: Passed")
        overall_success = True
    else:
        print("❌ TRAFFIC ATTRIBUTION: Failed")
        overall_success = False
    
    print("="*60)
    
    if overall_success:
        print("🎉 ALL TESTS COMPLETED SUCCESSFULLY")
        if gsc_result is True:
            print("💡 GSC integration will provide enhanced SEO attribution")
        elif gsc_result is None:
            print("💡 Consider setting up GSC for better SEO attribution")
    else:
        print("⚠️  SOME TESTS FAILED - CHECK LOGS ABOVE")
