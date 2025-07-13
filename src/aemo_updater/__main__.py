#!/usr/bin/env python3
"""
AEMO Data Updater CLI Entry Point
"""

import sys
import argparse
from datetime import datetime

from .config import get_logger
from .service import AemoDataService
from .integrity import run_integrity_check

logger = get_logger(__name__)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='AEMO Data Updater')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Service command
    service_parser = subparsers.add_parser('service', help='Run the data collection service')
    
    # UI command
    ui_parser = subparsers.add_parser('ui', help='Run the status monitoring UI')
    
    # Integrity check command
    integrity_parser = subparsers.add_parser('integrity', help='Check data integrity')
    
    # Backfill command
    backfill_parser = subparsers.add_parser('backfill', help='Backfill historical data')
    backfill_parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    backfill_parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    backfill_parser.add_argument('--collectors', nargs='+', 
                                help='Specific collectors to backfill (default: all available)')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show current status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'service':
        logger.info("Starting AEMO Data Updater Service...")
        service = AemoDataService()
        service.run_continuous()
        
    elif args.command == 'ui':
        logger.info("Starting Status UI...")
        from .ui.status_dashboard import run_dashboard
        run_dashboard()
        
    elif args.command == 'integrity':
        logger.info("Running integrity check...")
        service = AemoDataService()
        results = service.run_integrity_check()
        
        print("\n=== Data Integrity Check Results ===")
        for name, result in results.items():
            status = result.get('status', 'Unknown')
            print(f"\n{name.upper()}: {status}")
            
            if result.get('issues'):
                for issue in result['issues']:
                    print(f"  ⚠️  {issue}")
            
            if 'records' in result:
                print(f"  📊 Records: {result['records']}")
            if 'date_range' in result:
                print(f"  📅 Date range: {result['date_range']}")
        
    elif args.command == 'backfill':
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None
        
        logger.info("Running backfill...")
        service = AemoDataService()
        results = service.run_backfill(start_date, end_date, args.collectors)
        
        print("\n=== Backfill Results ===")
        for name, success in results.items():
            status = "✅ Success" if success else "❌ Failed/Not Available"
            print(f"{name}: {status}")
        
    elif args.command == 'status':
        service = AemoDataService()
        status = service.get_service_status()
        
        print("\n=== AEMO Data Updater Status ===")
        print(f"Service: {status['service_status']}")
        print(f"Last check: {status['last_update']}")
        
        print("\nCollectors:")
        for name, collector_status in status['collectors'].items():
            print(f"\n{name.upper()}:")
            print(f"  Status: {collector_status.get('status', 'Unknown')}")
            if 'records' in collector_status:
                print(f"  Records: {collector_status['records']:,}")
            if 'latest_update' in collector_status:
                print(f"  Latest: {collector_status['latest_update']}")
            if 'file_size_mb' in collector_status:
                print(f"  File size: {collector_status['file_size_mb']:.2f} MB")


if __name__ == "__main__":
    main()