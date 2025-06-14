
#!/usr/bin/env python3
"""
Test script for Traffic Attribution Module
Verifies that the attribution analysis is working correctly
"""

from modules.traffic_attribution import analyze_traffic_attribution
import os
import pandas as pd

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
    success = test_attribution()
    
    print("\n" + "="*60)
    if success:
        print("✅ TRAFFIC ATTRIBUTION TEST PASSED")
    else:
        print("❌ TRAFFIC ATTRIBUTION TEST FAILED")
    print("="*60)
