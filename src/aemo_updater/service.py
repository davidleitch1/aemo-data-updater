#!/usr/bin/env python3
"""
AEMO Data Updater Service
Main service that orchestrates all data collectors
"""

import time
from datetime import datetime
from typing import Dict, Any

from .config import get_config, get_logger
from .collectors.generation_collector import GenerationCollector
from .collectors.transmission_collector import TransmissionCollector
from .collectors.price_collector import PriceCollector
from .collectors.rooftop_collector import RooftopCollector

logger = get_logger(__name__)


class AemoDataService:
    """Main service that orchestrates all data collection"""
    
    def __init__(self):
        self.config = get_config()
        self.update_interval = self.config.update_interval_minutes * 60  # Convert to seconds
        
        # Initialize all collectors
        logger.info("Initializing collectors...")
        self.collectors = {}
        
        try:
            self.collectors['generation'] = GenerationCollector()
            logger.info("Generation collector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize generation collector: {e}")
            
        try:
            self.collectors['transmission'] = TransmissionCollector()
            logger.info("Transmission collector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize transmission collector: {e}")
            
        try:
            self.collectors['prices'] = PriceCollector()
            logger.info("Price collector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize price collector: {e}")
            
        try:
            self.collectors['rooftop'] = RooftopCollector()
            logger.info("Rooftop solar collector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize rooftop collector: {e}")
            
        logger.info(f"Service initialized with {len(self.collectors)} collectors")
        
    def run_single_update(self) -> Dict[str, bool]:
        """Run single update cycle for all collectors"""
        logger.info("=== Starting combined update cycle ===")
        start_time = datetime.now()
        
        results = {}
        
        for name, collector in self.collectors.items():
            try:
                logger.info(f"Running {name} update...")
                success = collector.update()
                results[name] = success
                logger.info(f"{name} update: {'succeeded' if success else 'completed (no new data)'}")
            except Exception as e:
                logger.error(f"Error in {name} update: {e}")
                results[name] = False
        
        # Log cycle summary
        duration = (datetime.now() - start_time).total_seconds()
        success_count = sum(results.values())
        total_collectors = len(results)
        
        logger.info(f"=== Combined update cycle complete in {duration:.1f}s ===")
        logger.info(f"Results: {success_count}/{total_collectors} collectors updated")
        
        # Log individual results
        status_symbols = {'generation': '🏭', 'transmission': '⚡', 'prices': '💰', 'rooftop': '☀️'}
        status_line = " | ".join([
            f"{status_symbols.get(name, '📊')} {name}: {'✓' if success else '○'}"
            for name, success in results.items()
        ])
        logger.info(status_line)
        
        return results
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get status summary for all collectors"""
        status = {
            'service_status': 'running',
            'collectors': {},
            'last_update': datetime.now()
        }
        
        for name, collector in self.collectors.items():
            try:
                collector_summary = collector.get_summary()
                status['collectors'][name] = collector_summary
            except Exception as e:
                logger.error(f"Error getting {name} status: {e}")
                status['collectors'][name] = {
                    'status': 'Error',
                    'error': str(e)
                }
        
        return status
    
    def run_integrity_check(self) -> Dict[str, Any]:
        """Run integrity check on all collectors"""
        logger.info("Running integrity check on all data...")
        integrity_results = {}
        
        for name, collector in self.collectors.items():
            try:
                logger.info(f"Checking {name} data integrity...")
                result = collector.check_integrity()
                integrity_results[name] = result
                logger.info(f"{name} integrity: {result['status']}")
                
                if result.get('issues'):
                    for issue in result['issues']:
                        logger.warning(f"{name}: {issue}")
                        
            except Exception as e:
                logger.error(f"Error checking {name} integrity: {e}")
                integrity_results[name] = {
                    'status': 'Error',
                    'error': str(e)
                }
        
        return integrity_results
    
    def run_backfill(self, start_date=None, end_date=None, collectors=None) -> Dict[str, bool]:
        """Run backfill on specified collectors"""
        if collectors is None:
            collectors = list(self.collectors.keys())
        
        logger.info(f"Running backfill for collectors: {collectors}")
        if start_date:
            logger.info(f"Start date: {start_date}")
        if end_date:
            logger.info(f"End date: {end_date}")
        
        backfill_results = {}
        
        for name in collectors:
            if name not in self.collectors:
                logger.warning(f"Collector {name} not found")
                backfill_results[name] = False
                continue
                
            collector = self.collectors[name]
            try:
                logger.info(f"Running backfill for {name}...")
                success = collector.backfill(start_date, end_date)
                backfill_results[name] = success
                logger.info(f"{name} backfill: {'succeeded' if success else 'failed/not available'}")
            except Exception as e:
                logger.error(f"Error in {name} backfill: {e}")
                backfill_results[name] = False
        
        return backfill_results
    
    def run_continuous(self):
        """Run continuous update loop"""
        logger.info("Starting AEMO combined data monitoring...")
        logger.info(f"Checking every {self.update_interval/60:.1f} minutes for new data")
        
        # Log initial status
        try:
            initial_status = self.get_service_status()
            logger.info("Initial data status:")
            for name, status in initial_status['collectors'].items():
                if 'records' in status:
                    logger.info(f"  {name}: {status['records']} records, latest: {status.get('latest_update', 'N/A')}")
        except Exception as e:
            logger.error(f"Error getting initial status: {e}")
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info(f"--- Cycle {cycle_count} ---")
                
                # Run combined update
                results = self.run_single_update()
                
                # If any collector succeeded, log updated status
                if any(results.values()):
                    try:
                        status = self.get_service_status()
                        logger.info("Updated data status:")
                        for name, collector_status in status['collectors'].items():
                            if 'records' in collector_status and results.get(name, False):
                                logger.info(f"  {name}: {collector_status['records']} records, "
                                          f"latest: {collector_status.get('latest_update', 'N/A')}")
                    except Exception as e:
                        logger.error(f"Error getting updated status: {e}")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
            
            # Wait for next update interval
            logger.info(f"Waiting {self.update_interval/60:.1f} minutes for next check...")
            time.sleep(self.update_interval)


def main():
    """Main function to run the combined data service"""
    logger.info("AEMO Combined Data Updater Service starting...")
    
    # Create service instance
    service = AemoDataService()
    
    try:
        service.run_continuous()
    except KeyboardInterrupt:
        logger.info("Combined monitoring stopped by user")
    except Exception as e:
        logger.error(f"Service crashed: {e}")


if __name__ == "__main__":
    main()