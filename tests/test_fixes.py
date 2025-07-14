#!/usr/bin/env python3
"""
Test fixes for transmission URL pattern and generation file path
"""

import sys
import os
import requests
import re
from pathlib import Path
import pandas as pd

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

print("🔧 Testing Configuration Fixes")
print("="*50)

def test_transmission_url_pattern():
    """Test transmission data URL access with correct pattern"""
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Use the correct pattern from working version: 'DISPATCHIS' in href
        zip_files = []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.zip') and 'DISPATCHIS' in href:
                zip_files.append(href)
        
        if zip_files:
            latest_file = sorted(zip_files)[-1]
            print(f"✅ Transmission URL Access FIXED: Found {len(zip_files)} files")
            print(f"    Latest: {latest_file}")
            return True
        else:
            print("❌ Transmission URL Access: Still no files found")
            return False
            
    except Exception as e:
        print(f"❌ Transmission URL Access: Error - {e}")
        return False

def test_generation_file_paths():
    """Test generation file paths - find where the actual file is"""
    print("\n🔍 Generation File Path Analysis:")
    
    # Check all possible locations
    data_paths = [
        "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot",
        "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist", 
        "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data"
    ]
    
    found_files = []
    
    for base_path in data_paths:
        path = Path(base_path)
        if path.exists():
            # Look for generation files
            for pattern in ['*gen*output*.parquet', '*generation*.parquet', 'gen_output.parquet']:
                for file in path.glob(f"**/{pattern}"):
                    file_size = file.stat().st_size / (1024*1024)  # MB
                    
                    # Check if it has data
                    try:
                        df = pd.read_parquet(file)
                        records = len(df)
                        found_files.append({
                            'path': file,
                            'size_mb': file_size,
                            'records': records,
                            'latest_date': df.get('settlementdate', pd.Series()).max() if 'settlementdate' in df.columns else 'N/A'
                        })
                        print(f"    📁 {file}")
                        print(f"        Size: {file_size:.1f} MB, Records: {records:,}")
                        print(f"        Latest: {df.get('settlementdate', pd.Series()).max() if 'settlementdate' in df.columns else 'N/A'}")
                    except Exception as e:
                        print(f"    📁 {file} (Error reading: {e})")
    
    if found_files:
        # Find the most recent/largest file
        best_file = max(found_files, key=lambda x: (x['records'], x['size_mb']))
        print(f"\n✅ Generation File Found: {best_file['path']}")
        print(f"    Best choice: {best_file['records']:,} records, {best_file['size_mb']:.1f} MB")
        return str(best_file['path'])
    else:
        print("❌ Generation File: No parquet files found")
        return None

def test_config_compatibility():
    """Test the new config against expected paths"""
    print("\n⚙️  Testing New Config:")
    
    try:
        from aemo_updater.config import get_config
        config = get_config()
        
        print(f"    gen_output_file: {config.gen_output_file}")
        print(f"    transmission_output_file: {config.transmission_output_file}")
        print(f"    spot_hist_file: {config.spot_hist_file}")
        print(f"    rooftop_solar_file: {config.rooftop_solar_file}")
        
        # Test if files exist
        files_exist = {
            'generation': config.gen_output_file.exists() if hasattr(config.gen_output_file, 'exists') else Path(config.gen_output_file).exists(),
            'transmission': config.transmission_output_file.exists() if hasattr(config.transmission_output_file, 'exists') else Path(config.transmission_output_file).exists(),
            'prices': config.spot_hist_file.exists() if hasattr(config.spot_hist_file, 'exists') else Path(config.spot_hist_file).exists(),
            'rooftop': config.rooftop_solar_file.exists() if hasattr(config.rooftop_solar_file, 'exists') else Path(config.rooftop_solar_file).exists()
        }
        
        print(f"\n    File Existence Check:")
        for name, exists in files_exist.items():
            status = "✅" if exists else "❌"
            print(f"      {status} {name}: {exists}")
        
        return all(files_exist.values())
        
    except Exception as e:
        print(f"❌ Config Test Error: {e}")
        return False

def main():
    """Run fix tests"""
    results = []
    
    # Test transmission fix
    results.append(test_transmission_url_pattern())
    
    # Test generation file paths
    best_gen_path = test_generation_file_paths()
    results.append(best_gen_path is not None)
    
    # Test new config
    results.append(test_config_compatibility())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n{'='*50}")
    print(f"FIX TEST SUMMARY: {passed}/{total} tests passed")
    print(f"{'='*50}")
    
    if passed == total:
        print("🎉 All fixes working! Ready to update main config.")
        if best_gen_path:
            print(f"\n📋 Recommended config update:")
            print(f"   GEN_OUTPUT_FILE={best_gen_path}")
    else:
        print("⚠️  Some fixes still needed.")
    
    return passed == total

if __name__ == "__main__":
    sys.exit(0 if main() else 1)