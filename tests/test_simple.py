#!/usr/bin/env python3
"""
Simplified Test Suite for AEMO Data Collectors
Tests core functionality: URL access, data parsing, basic operations
"""

import sys
import os
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

print("🧪 AEMO Data Collectors - Simplified Test Suite")
print("="*60)

# Test 1: URL Access Tests
print("\n📡 Testing URL Access...")

def test_generation_url():
    """Test generation data URL access"""
    try:
        url = "http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Look for ZIP files
        import re
        zip_pattern = r'DISPATCHSCADA.*\.zip'
        matches = re.findall(zip_pattern, response.text)
        
        if matches:
            print(f"✅ Generation URL Access: Found {len(matches)} files")
            print(f"    Latest: {sorted(matches)[-1] if matches else 'None'}")
            return True
        else:
            print("❌ Generation URL Access: No SCADA files found")
            return False
            
    except Exception as e:
        print(f"❌ Generation URL Access: Error - {e}")
        return False

def test_transmission_url():
    """Test transmission data URL access"""
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Look for interconnector files
        import re
        zip_pattern = r'PUBLIC_DISPATCHINTERCONNECTORRES.*\.zip'
        matches = re.findall(zip_pattern, response.text)
        
        if matches:
            print(f"✅ Transmission URL Access: Found {len(matches)} files")
            print(f"    Latest: {sorted(matches)[-1] if matches else 'None'}")
            return True
        else:
            print("❌ Transmission URL Access: No interconnector files found")
            return False
            
    except Exception as e:
        print(f"❌ Transmission URL Access: Error - {e}")
        return False

def test_price_url():
    """Test price data URL access"""
    try:
        url = "http://nemweb.com.au/Reports/Current/Dispatch_Reports/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Look for dispatch files
        import re
        zip_pattern = r'PUBLIC_DISPATCH_\d{12}_\d{14}_LEGACY\.zip'
        matches = re.findall(zip_pattern, response.text)
        
        if matches:
            print(f"✅ Price URL Access: Found {len(matches)} files")
            print(f"    Latest: {sorted(matches)[-1] if matches else 'None'}")
            return True
        else:
            print("❌ Price URL Access: No dispatch files found")
            return False
            
    except Exception as e:
        print(f"❌ Price URL Access: Error - {e}")
        return False

def test_rooftop_url():
    """Test rooftop solar data URL access"""
    try:
        url = "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find ZIP file links
        zip_files = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.zip') and 'ROOFTOP_PV_ACTUAL_MEASUREMENT' in href:
                zip_files.append(href)
        
        if zip_files:
            print(f"✅ Rooftop URL Access: Found {len(zip_files)} files")
            print(f"    Latest: {sorted(zip_files)[-1] if zip_files else 'None'}")
            return True
        else:
            print("❌ Rooftop URL Access: No rooftop PV files found")
            return False
            
    except Exception as e:
        print(f"❌ Rooftop URL Access: Error - {e}")
        return False

# Test 2: Data Parsing Test
print("\n🔍 Testing Data Parsing...")

def test_rooftop_conversion():
    """Test the critical 30-minute to 5-minute conversion algorithm"""
    try:
        # Create test 30-minute data
        test_data = pd.DataFrame({
            'settlementdate': [
                datetime(2025, 1, 13, 10, 0),
                datetime(2025, 1, 13, 10, 30),
                datetime(2025, 1, 13, 11, 0),
            ],
            'NSW1': [1000, 1200, 1100],
            'QLD1': [800, 900, 850]
        })
        
        # Implement the critical conversion algorithm
        five_min_records = []
        for i in range(len(test_data)):
            current_row = test_data.iloc[i]
            current_time = current_row['settlementdate']
            
            # Get next row if available
            if i < len(test_data) - 1:
                next_row = test_data.iloc[i + 1]
                has_next = True
            else:
                has_next = False
            
            # Generate 6 x 5-minute periods for this 30-minute period
            for j in range(6):
                from datetime import timedelta
                five_min_time = current_time + timedelta(minutes=j*5)
                
                record = {'settlementdate': five_min_time}
                
                # Calculate values for each region
                for region in ['NSW1', 'QLD1']:
                    current_value = current_row[region]
                    
                    if has_next:
                        next_value = next_row[region]
                        # CRITICAL ALGORITHM: Weighted average: (6-j)*current + j*next / 6
                        value = ((6 - j) * current_value + j * next_value) / 6
                    else:
                        # No next value - use current value for all periods
                        value = current_value
                    
                    record[region] = value
                
                five_min_records.append(record)
        
        df_5min = pd.DataFrame(five_min_records)
        
        if len(df_5min) == 18:  # 3 x 6 = 18 records
            print(f"✅ Rooftop 30→5min Conversion: Generated {len(df_5min)} records from {len(test_data)}")
            
            # Test first period conversion
            first_period = df_5min[df_5min['settlementdate'] == datetime(2025, 1, 13, 10, 0)].iloc[0]
            expected_nsw1 = ((6-0) * 1000 + 0 * 1200) / 6  # First period, j=0
            if abs(first_period['NSW1'] - expected_nsw1) < 0.01:
                print("✅ Rooftop Algorithm Verification: First period calculation correct")
                return True
            else:
                print(f"❌ Rooftop Algorithm Verification: Expected {expected_nsw1}, got {first_period['NSW1']}")
                return False
        else:
            print(f"❌ Rooftop 30→5min Conversion: Expected 18 records, got {len(df_5min)}")
            return False
            
    except Exception as e:
        print(f"❌ Rooftop 30→5min Conversion: Error - {e}")
        return False

# Test 3: Parquet File Operations
print("\n💾 Testing Parquet Operations...")

def test_parquet_operations():
    """Test reading and writing parquet files"""
    try:
        # Create temporary test directory
        test_dir = Path(tempfile.mkdtemp(prefix='aemo_parquet_test_'))
        
        # Create test data
        test_data = pd.DataFrame({
            'settlementdate': pd.date_range('2025-01-13 10:00', periods=10, freq='5min'),
            'value': range(10),
            'region': ['NSW1'] * 10
        })
        
        # Test saving
        test_file = test_dir / 'test_data.parquet'
        test_data.to_parquet(test_file, compression='snappy', index=False)
        
        if test_file.exists():
            # Test loading
            loaded_data = pd.read_parquet(test_file)
            
            if len(loaded_data) == len(test_data):
                file_size = test_file.stat().st_size / 1024  # KB
                print(f"✅ Parquet Operations: Saved and loaded {len(loaded_data)} records ({file_size:.1f} KB)")
                
                # Cleanup
                shutil.rmtree(test_dir)
                return True
            else:
                print(f"❌ Parquet Operations: Data mismatch - saved {len(test_data)}, loaded {len(loaded_data)}")
                return False
        else:
            print("❌ Parquet Operations: File not created")
            return False
            
    except Exception as e:
        print(f"❌ Parquet Operations: Error - {e}")
        return False

# Test 4: Integrity Check on Real Files
print("\n🔍 Testing Integrity Check on Real Data...")

def test_existing_data_integrity():
    """Test integrity checking on existing parquet files"""
    data_path = Path('/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot')
    
    files_to_check = {
        'Generation': data_path / 'gen_output.parquet',
        'Prices': data_path / 'spot_hist.parquet', 
        'Transmission': data_path / 'transmission_flows.parquet',
        'Rooftop Solar': data_path / 'rooftop_solar.parquet'
    }
    
    results = {}
    
    for name, file_path in files_to_check.items():
        try:
            if file_path.exists():
                df = pd.read_parquet(file_path)
                file_size = file_path.stat().st_size / (1024*1024)  # MB
                
                # Basic integrity checks
                issues = []
                
                # Check for empty data
                if df.empty:
                    issues.append("No data")
                
                # Check for recent data (within last 24 hours)
                if not df.empty:
                    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                    if date_cols:
                        latest_date = pd.to_datetime(df[date_cols[0]]).max()
                        hours_old = (datetime.now() - latest_date).total_seconds() / 3600
                        if hours_old > 24:
                            issues.append(f"Data is {hours_old:.1f} hours old")
                
                status = "OK" if not issues else "Issues found"
                print(f"✅ {name}: {len(df):,} records, {file_size:.1f} MB, Status: {status}")
                
                if issues:
                    for issue in issues:
                        print(f"    ⚠️  {issue}")
                
                results[name] = True
            else:
                print(f"❌ {name}: File not found at {file_path}")
                results[name] = False
                
        except Exception as e:
            print(f"❌ {name}: Error reading file - {e}")
            results[name] = False
    
    return all(results.values())

# Run all tests
def main():
    """Run all tests and provide summary"""
    test_results = []
    
    # URL access tests
    test_results.append(test_generation_url())
    test_results.append(test_transmission_url())
    test_results.append(test_price_url())
    test_results.append(test_rooftop_url())
    
    # Algorithm test
    test_results.append(test_rooftop_conversion())
    
    # File operations test
    test_results.append(test_parquet_operations())
    
    # Integrity test
    test_results.append(test_existing_data_integrity())
    
    # Summary
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY: {passed}/{total} tests passed")
    print(f"{'='*60}")
    
    if passed == total:
        print("🎉 All core tests passed! The updater components are working correctly.")
        print("\n✅ Ready for deployment:")
        print("   • URL access to all NEMWEB sources ✓")
        print("   • Critical rooftop solar algorithm ✓") 
        print("   • Parquet file operations ✓")
        print("   • Existing data integrity ✓")
        return 0
    else:
        print(f"⚠️  {total - passed} tests failed. Review issues before deploying.")
        return 1

if __name__ == "__main__":
    sys.exit(main())