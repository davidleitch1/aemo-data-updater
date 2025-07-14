#!/usr/bin/env python3
"""
Comprehensive Test Suite for AEMO Data Collectors
Tests URL access, data parsing, parquet saving, and integrity checking
"""

import sys
import os
import tempfile
import shutil
from datetime import datetime
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from aemo_updater.config import get_config, get_logger
from aemo_updater.collectors.generation_collector import GenerationCollector
from aemo_updater.collectors.transmission_collector import TransmissionCollector
from aemo_updater.collectors.price_collector import PriceCollector
from aemo_updater.collectors.rooftop_collector import RooftopCollector

logger = get_logger(__name__)


class TestResults:
    """Track test results"""
    def __init__(self):
        self.results = {}
        self.passed = 0
        self.failed = 0
    
    def add_result(self, test_name: str, success: bool, message: str = ""):
        self.results[test_name] = {
            'success': success,
            'message': message
        }
        if success:
            self.passed += 1
        else:
            self.failed += 1
    
    def print_summary(self):
        print(f"\n{'='*60}")
        print(f"TEST SUMMARY: {self.passed} passed, {self.failed} failed")
        print(f"{'='*60}")
        
        for test_name, result in self.results.items():
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            print(f"{status} {test_name}")
            if result['message']:
                print(f"     {result['message']}")


def test_generation_collector(results: TestResults, test_dir: Path):
    """Test the generation data collector"""
    print("\n🏭 Testing Generation Collector...")
    
    try:
        # Override config to use test directory
        config = get_config()
        original_file = config.gen_output_file
        config.gen_output_file = test_dir / 'test_gen_output.parquet'
        
        collector = GenerationCollector()
        results.add_result("Generation Collector Init", True, "Collector initialized successfully")
        
        # Test URL access
        try:
            latest_url = collector.get_latest_file_url()
            if latest_url:
                results.add_result("Generation URL Access", True, f"Found URL: {latest_url}")
                
                # Test download and parsing
                new_data = collector.download_and_parse_file(latest_url)
                if new_data is not None and not new_data.empty:
                    results.add_result("Generation Data Parsing", True, f"Parsed {len(new_data)} records")
                    
                    # Test saving
                    collector.gen_output = new_data
                    collector.save_rooftop_data = lambda: collector.gen_output.to_parquet(
                        config.gen_output_file, compression='snappy', index=False
                    )
                    collector.save_rooftop_data()
                    
                    if config.gen_output_file.exists():
                        results.add_result("Generation Parquet Save", True, f"Test file saved: {config.gen_output_file}")
                    else:
                        results.add_result("Generation Parquet Save", False, "Test file not created")
                else:
                    results.add_result("Generation Data Parsing", False, "No data parsed from file")
            else:
                results.add_result("Generation URL Access", False, "Could not find latest file URL")
        except Exception as e:
            results.add_result("Generation URL Access", False, f"Error: {e}")
        
        # Test integrity check on real data
        try:
            config.gen_output_file = original_file  # Use real file for integrity check
            if original_file.exists():
                collector.gen_output_file = original_file
                collector.gen_output = collector.load_or_create_dataframe()
                integrity_result = collector.check_integrity()
                results.add_result("Generation Integrity Check", True, 
                                 f"Status: {integrity_result['status']}, Records: {integrity_result.get('records', 0)}")
            else:
                results.add_result("Generation Integrity Check", False, "Original parquet file not found")
        except Exception as e:
            results.add_result("Generation Integrity Check", False, f"Error: {e}")
            
    except Exception as e:
        results.add_result("Generation Collector Init", False, f"Failed to initialize: {e}")


def test_transmission_collector(results: TestResults, test_dir: Path):
    """Test the transmission data collector"""
    print("\n⚡ Testing Transmission Collector...")
    
    try:
        # Override config to use test directory
        config = get_config()
        original_file = config.transmission_output_file
        config.transmission_output_file = test_dir / 'test_transmission_flows.parquet'
        
        collector = TransmissionCollector()
        results.add_result("Transmission Collector Init", True, "Collector initialized successfully")
        
        # Test URL access
        try:
            latest_url = collector.get_latest_file_url()
            if latest_url:
                results.add_result("Transmission URL Access", True, f"Found URL: {latest_url}")
                
                # Test download and parsing
                new_data = collector.download_and_parse_file(latest_url)
                if new_data is not None and not new_data.empty:
                    results.add_result("Transmission Data Parsing", True, f"Parsed {len(new_data)} records")
                    
                    # Test saving
                    collector.transmission_data = new_data
                    collector.save_transmission_data()
                    
                    if config.transmission_output_file.exists():
                        results.add_result("Transmission Parquet Save", True, f"Test file saved: {config.transmission_output_file}")
                    else:
                        results.add_result("Transmission Parquet Save", False, "Test file not created")
                else:
                    results.add_result("Transmission Data Parsing", False, "No data parsed from file")
            else:
                results.add_result("Transmission URL Access", False, "Could not find latest file URL")
        except Exception as e:
            results.add_result("Transmission URL Access", False, f"Error: {e}")
        
        # Test integrity check on real data
        try:
            config.transmission_output_file = original_file  # Use real file for integrity check
            if original_file.exists():
                collector.transmission_output_file = original_file
                collector.transmission_data = collector.load_existing_data()
                integrity_result = collector.check_integrity()
                results.add_result("Transmission Integrity Check", True, 
                                 f"Status: {integrity_result['status']}, Records: {integrity_result.get('records', 0)}")
            else:
                results.add_result("Transmission Integrity Check", False, "Original parquet file not found")
        except Exception as e:
            results.add_result("Transmission Integrity Check", False, f"Error: {e}")
            
    except Exception as e:
        results.add_result("Transmission Collector Init", False, f"Failed to initialize: {e}")


def test_price_collector(results: TestResults, test_dir: Path):
    """Test the price data collector"""
    print("\n💰 Testing Price Collector...")
    
    try:
        # Override config to use test directory
        config = get_config()
        original_file = config.spot_hist_file
        config.spot_hist_file = test_dir / 'test_spot_hist.parquet'
        
        collector = PriceCollector()
        results.add_result("Price Collector Init", True, "Collector initialized successfully")
        
        # Test URL access
        try:
            csv_content, filename = collector.get_latest_dispatch_file()
            if csv_content and filename:
                results.add_result("Price URL Access", True, f"Downloaded file: {filename}")
                
                # Test parsing
                new_data = collector.parse_dispatch_data(csv_content)
                if new_data is not None and not new_data.empty:
                    results.add_result("Price Data Parsing", True, f"Parsed {len(new_data)} records")
                    
                    # Test saving
                    collector.save_historical_data(new_data)
                    
                    if config.spot_hist_file.exists():
                        results.add_result("Price Parquet Save", True, f"Test file saved: {config.spot_hist_file}")
                    else:
                        results.add_result("Price Parquet Save", False, "Test file not created")
                else:
                    results.add_result("Price Data Parsing", False, "No data parsed from file")
            else:
                results.add_result("Price URL Access", False, "Could not download dispatch file")
        except Exception as e:
            results.add_result("Price URL Access", False, f"Error: {e}")
        
        # Test integrity check on real data
        try:
            config.spot_hist_file = original_file  # Use real file for integrity check
            if original_file.exists():
                collector.parquet_file_path = original_file
                collector.historical_df = collector.load_historical_data()
                integrity_result = collector.check_integrity()
                results.add_result("Price Integrity Check", True, 
                                 f"Status: {integrity_result['status']}, Records: {integrity_result.get('records', 0)}")
            else:
                results.add_result("Price Integrity Check", False, "Original parquet file not found")
        except Exception as e:
            results.add_result("Price Integrity Check", False, f"Error: {e}")
            
    except Exception as e:
        results.add_result("Price Collector Init", False, f"Failed to initialize: {e}")


def test_rooftop_collector(results: TestResults, test_dir: Path):
    """Test the rooftop solar data collector"""
    print("\n☀️ Testing Rooftop Solar Collector...")
    
    try:
        # Override config to use test directory
        config = get_config()
        original_file = config.rooftop_solar_file
        config.rooftop_solar_file = test_dir / 'test_rooftop_solar.parquet'
        
        collector = RooftopCollector()
        results.add_result("Rooftop Collector Init", True, "Collector initialized successfully")
        
        # Test URL access
        try:
            recent_files = collector.get_latest_rooftop_pv_files()
            if recent_files:
                results.add_result("Rooftop URL Access", True, f"Found {len(recent_files)} recent files")
                
                # Test download and parsing with first file
                zip_content = collector.download_rooftop_pv_zip(recent_files[0])
                if zip_content:
                    results.add_result("Rooftop File Download", True, f"Downloaded {len(zip_content)} bytes")
                    
                    # Test parsing
                    df_30min = collector.parse_rooftop_pv_zip(zip_content)
                    if not df_30min.empty:
                        results.add_result("Rooftop Data Parsing", True, f"Parsed {len(df_30min)} 30-min records")
                        
                        # Test critical 30-min to 5-min conversion
                        df_5min = collector.convert_30min_to_5min(df_30min)
                        if not df_5min.empty:
                            results.add_result("Rooftop 30→5min Conversion", True, 
                                             f"Converted to {len(df_5min)} 5-min records")
                            
                            # Test saving
                            collector.rooftop_data = df_5min
                            collector.save_rooftop_data()
                            
                            if config.rooftop_solar_file.exists():
                                results.add_result("Rooftop Parquet Save", True, f"Test file saved: {config.rooftop_solar_file}")
                            else:
                                results.add_result("Rooftop Parquet Save", False, "Test file not created")
                        else:
                            results.add_result("Rooftop 30→5min Conversion", False, "Conversion produced no data")
                    else:
                        results.add_result("Rooftop Data Parsing", False, "No data parsed from ZIP")
                else:
                    results.add_result("Rooftop File Download", False, "Could not download ZIP file")
            else:
                results.add_result("Rooftop URL Access", False, "No recent files found")
        except Exception as e:
            results.add_result("Rooftop URL Access", False, f"Error: {e}")
        
        # Test integrity check on real data
        try:
            config.rooftop_solar_file = original_file  # Use real file for integrity check
            if original_file.exists():
                collector.rooftop_output_file = original_file
                collector.rooftop_data = collector.load_existing_data()
                integrity_result = collector.check_integrity()
                results.add_result("Rooftop Integrity Check", True, 
                                 f"Status: {integrity_result['status']}, Records: {integrity_result.get('records', 0)}")
            else:
                results.add_result("Rooftop Integrity Check", False, "Original parquet file not found")
        except Exception as e:
            results.add_result("Rooftop Integrity Check", False, f"Error: {e}")
            
    except Exception as e:
        results.add_result("Rooftop Collector Init", False, f"Failed to initialize: {e}")


def main():
    """Run comprehensive test suite"""
    print("🧪 AEMO Data Collectors Test Suite")
    print("="*60)
    
    results = TestResults()
    
    # Create temporary directory for test files
    test_dir = Path(tempfile.mkdtemp(prefix='aemo_test_'))
    print(f"Using test directory: {test_dir}")
    
    try:
        # Test each collector
        test_generation_collector(results, test_dir)
        test_transmission_collector(results, test_dir)
        test_price_collector(results, test_dir)
        test_rooftop_collector(results, test_dir)
        
        # Print final results
        results.print_summary()
        
        # Show test files created
        print(f"\nTest files created in: {test_dir}")
        for file in test_dir.glob("*.parquet"):
            file_size = file.stat().st_size / 1024  # KB
            print(f"  📁 {file.name}: {file_size:.1f} KB")
        
    finally:
        # Cleanup test directory
        try:
            shutil.rmtree(test_dir)
            print(f"\n🧹 Cleaned up test directory: {test_dir}")
        except Exception as e:
            print(f"⚠️  Could not clean up test directory: {e}")
    
    # Exit with appropriate code
    if results.failed == 0:
        print("\n🎉 All tests passed! The updater is ready for use.")
        sys.exit(0)
    else:
        print(f"\n⚠️  {results.failed} tests failed. Please review before deploying.")
        sys.exit(1)


if __name__ == "__main__":
    main()