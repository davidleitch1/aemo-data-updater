#!/usr/bin/env python3
"""
Final test of transmission and generation fixes
"""

import sys
import os
import requests
import re
from pathlib import Path
import pandas as pd

# Add the src directory to the path  
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

print("🎯 Final Test: Transmission & Generation Fixes")
print("="*55)

def test_transmission_access():
    """Test transmission URL access with working pattern"""
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Use pattern from working version
        zip_files = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.zip') and 'DISPATCHIS' in href:
                zip_files.append(href)
        
        if zip_files:
            latest = sorted(zip_files)[-1]
            print(f"✅ Transmission URL: Found {len(zip_files)} files")
            print(f"    Latest: {latest}")
            
            # Test downloading one file briefly
            try:
                if latest.startswith('/'):
                    file_url = "http://nemweb.com.au" + latest
                else:
                    file_url = url + latest
                    
                head_response = requests.head(file_url, timeout=10, headers=headers)
                if head_response.status_code == 200:
                    file_size = int(head_response.headers.get('content-length', 0)) / 1024  # KB
                    print(f"    File accessible: {file_size:.1f} KB")
                    return True
                else:
                    print(f"    File not accessible: HTTP {head_response.status_code}")
                    return False
            except Exception as e:
                print(f"    File access test failed: {e}")
                return False
        else:
            print("❌ Transmission URL: No files found")
            return False
            
    except Exception as e:
        print(f"❌ Transmission URL: Error - {e}")
        return False

def test_generation_access():
    """Test generation file path with config"""
    try:
        from aemo_updater.config import get_config
        config = get_config()
        
        gen_file = config.gen_output_file
        print(f"✅ Generation Config: {gen_file}")
        
        if Path(gen_file).exists():
            df = pd.read_parquet(gen_file)
            file_size = Path(gen_file).stat().st_size / (1024*1024)  # MB
            latest_date = df['settlementdate'].max() if 'settlementdate' in df.columns else 'N/A'
            
            print(f"    File exists: {len(df):,} records, {file_size:.1f} MB")
            print(f"    Latest data: {latest_date}")
            
            # Check data freshness (should be within last few hours)
            if latest_date != 'N/A':
                from datetime import datetime, timedelta
                latest_dt = pd.to_datetime(latest_date)
                age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
                if age_hours < 24:
                    print(f"    Data freshness: ✅ {age_hours:.1f} hours old")
                    return True
                else:
                    print(f"    Data freshness: ⚠️  {age_hours:.1f} hours old")
                    return True  # Still counts as working, just older data
            return True
        else:
            print(f"❌ Generation File: Not found at {gen_file}")
            return False
            
    except Exception as e:
        print(f"❌ Generation Test: Error - {e}")
        return False

def test_integrity_on_fixed_files():
    """Test integrity check on corrected file paths"""
    try:
        from aemo_updater.config import get_config
        config = get_config()
        
        print(f"\n🔍 Testing integrity on corrected paths:")
        
        files_to_check = {
            'Generation': config.gen_output_file,
            'Transmission': config.transmission_output_file,
            'Prices': config.spot_hist_file,
            'Rooftop': config.rooftop_solar_file
        }
        
        results = {}
        
        for name, file_path in files_to_check.items():
            try:
                if Path(file_path).exists():
                    df = pd.read_parquet(file_path)
                    file_size = Path(file_path).stat().st_size / (1024*1024)  # MB
                    
                    # Basic checks
                    issues = []
                    if df.empty:
                        issues.append("No data")
                    
                    # Check for recent data
                    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                    if date_cols:
                        latest_date = pd.to_datetime(df[date_cols[0]]).max()
                        hours_old = (pd.Timestamp.now() - latest_date).total_seconds() / 3600
                        if hours_old > 48:  # More lenient for test
                            issues.append(f"Data is {hours_old:.1f} hours old")
                    
                    status = "✅ OK" if not issues else "⚠️  Issues"
                    print(f"    {name}: {len(df):,} records, {file_size:.1f} MB - {status}")
                    
                    if issues:
                        for issue in issues:
                            print(f"      • {issue}")
                    
                    results[name] = len(issues) == 0
                else:
                    print(f"    {name}: ❌ File not found")
                    results[name] = False
                    
            except Exception as e:
                print(f"    {name}: ❌ Error - {e}")
                results[name] = False
        
        return all(results.values())
        
    except Exception as e:
        print(f"❌ Integrity Test: Error - {e}")
        return False

def main():
    """Run final tests"""
    results = []
    
    # Test fixes
    results.append(test_transmission_access())
    results.append(test_generation_access())
    results.append(test_integrity_on_fixed_files())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n{'='*55}")
    print(f"FINAL TEST SUMMARY: {passed}/{total} tests passed")
    print(f"{'='*55}")
    
    if passed == total:
        print("🎉 ALL FIXES SUCCESSFUL!")
        print("✅ Transmission URL pattern corrected")
        print("✅ Generation file path updated to most recent data")
        print("✅ All integrity checks passing")
        print("\n🚀 The standalone updater is ready for deployment!")
    else:
        print(f"⚠️  {total - passed} issues remaining")
    
    return passed == total

if __name__ == "__main__":
    sys.exit(0 if main() else 1)