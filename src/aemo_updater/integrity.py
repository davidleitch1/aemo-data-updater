"""
Data integrity checker for AEMO parquet files
Identifies gaps, validates data quality, and provides recommendations
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

from .config import PARQUET_FILES, QUALITY_THRESHOLDS


logger = logging.getLogger('aemo_updater.integrity')


class DataIntegrityChecker:
    """Check integrity of all AEMO data files"""
    
    def __init__(self):
        self.files = PARQUET_FILES
        self.thresholds = QUALITY_THRESHOLDS
        
    def check_file_integrity(self, name: str, config: Dict) -> Dict[str, Any]:
        """
        Check integrity of a single data file
        
        Returns:
            Dict with status, issues, and recommendations
        """
        file_path = Path(config['path'])
        result = {
            'name': name,
            'status': 'healthy',
            'issues': [],
            'recommendations': [],
            'details': {}
        }
        
        # Check if file exists
        if not file_path.exists():
            result['status'] = 'critical'
            result['issues'].append(f"File does not exist: {file_path}")
            result['recommendations'].append(f"Run backfill for {name} to create initial data")
            return result
            
        try:
            # Load data
            df = pd.read_parquet(file_path)
            
            if df.empty:
                result['status'] = 'critical'
                result['issues'].append("File exists but contains no data")
                result['recommendations'].append(f"Run backfill for {name}")
                return result
                
            # Basic statistics
            result['details']['records'] = len(df)
            result['details']['file_size_mb'] = file_path.stat().st_size / (1024 * 1024)
            
            # Check for time-based data
            if 'settlementdate' in df.columns:
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                
                # Date range
                date_min = df['settlementdate'].min()
                date_max = df['settlementdate'].max()
                result['details']['date_range'] = {
                    'start': date_min,
                    'end': date_max,
                    'days': (date_max - date_min).days
                }
                
                # Check data freshness
                age_minutes = (datetime.now() - date_max).total_seconds() / 60
                if age_minutes > self.thresholds['max_age_minutes']:
                    result['status'] = 'warning'
                    result['issues'].append(
                        f"Data is {age_minutes:.0f} minutes old "
                        f"(threshold: {self.thresholds['max_age_minutes']} minutes)"
                    )
                    result['recommendations'].append("Check if updater service is running")
                    
                # Check for gaps
                gaps = self._find_time_gaps(df, name)
                if gaps:
                    result['status'] = 'warning'
                    result['issues'].append(f"Found {len(gaps)} time gaps in data")
                    result['details']['gaps'] = gaps[:10]  # First 10 gaps
                    
                    # Recommend backfill for gap periods
                    if gaps:
                        earliest_gap = min(gap['start'] for gap in gaps)
                        latest_gap = max(gap['end'] for gap in gaps)
                        result['recommendations'].append(
                            f"Run backfill from {earliest_gap:%Y-%m-%d} to {latest_gap:%Y-%m-%d}"
                        )
                        
            # Source-specific checks
            if name == 'generation':
                self._check_generation_data(df, result)
            elif name == 'price':
                self._check_price_data(df, result)
            elif name == 'transmission':
                self._check_transmission_data(df, result)
            elif name == 'rooftop':
                self._check_rooftop_data(df, result)
                
        except Exception as e:
            result['status'] = 'error'
            result['issues'].append(f"Error reading file: {e}")
            logger.error(f"Error checking {name}: {e}")
            
        return result
        
    def _find_time_gaps(self, df: pd.DataFrame, source: str) -> List[Dict]:
        """Find gaps in time series data"""
        gaps = []
        
        # Expected interval based on source
        expected_interval = {
            'generation': timedelta(minutes=5),
            'price': timedelta(minutes=5),
            'transmission': timedelta(minutes=5),
            'rooftop': timedelta(minutes=5),  # After conversion
        }.get(source, timedelta(minutes=5))
        
        # Sort by time
        df = df.sort_values('settlementdate')
        
        # Group by key columns to check each series
        if source == 'generation' and 'duid' in df.columns:
            # Check gaps for each DUID
            for duid in df['duid'].unique()[:10]:  # Check first 10 DUIDs
                duid_df = df[df['duid'] == duid]
                duid_gaps = self._find_series_gaps(
                    duid_df['settlementdate'], 
                    expected_interval,
                    f"DUID {duid}"
                )
                gaps.extend(duid_gaps)
                
        elif source == 'transmission' and 'interconnectorid' in df.columns:
            # Check gaps for each interconnector
            for ic in df['interconnectorid'].unique():
                ic_df = df[df['interconnectorid'] == ic]
                ic_gaps = self._find_series_gaps(
                    ic_df['settlementdate'],
                    expected_interval,
                    f"Interconnector {ic}"
                )
                gaps.extend(ic_gaps)
                
        else:
            # Check overall gaps
            gaps = self._find_series_gaps(df['settlementdate'], expected_interval)
            
        return gaps
        
    def _find_series_gaps(self, 
                         timestamps: pd.Series, 
                         expected_interval: timedelta,
                         series_name: str = "") -> List[Dict]:
        """Find gaps in a single time series"""
        gaps = []
        
        # Calculate time differences
        time_diffs = timestamps.diff()
        
        # Find gaps (more than 1.5x expected interval)
        gap_threshold = expected_interval * 1.5
        gap_mask = time_diffs > gap_threshold
        
        if gap_mask.any():
            gap_indices = timestamps[gap_mask].index
            
            for idx in gap_indices[:10]:  # Limit to first 10 gaps
                gap_start = timestamps.iloc[timestamps.index.get_loc(idx) - 1]
                gap_end = timestamps.loc[idx]
                gap_duration = gap_end - gap_start
                
                gaps.append({
                    'series': series_name,
                    'start': gap_start,
                    'end': gap_end,
                    'duration': gap_duration,
                    'missing_intervals': int(gap_duration / expected_interval)
                })
                
        return gaps
        
    def _check_generation_data(self, df: pd.DataFrame, result: Dict):
        """Check generation-specific data quality"""
        if 'duid' in df.columns:
            unique_duids = df['duid'].nunique()
            result['details']['unique_duids'] = unique_duids
            
            # Check for expected minimum DUIDs
            if unique_duids < 400:
                result['status'] = 'warning'
                result['issues'].append(f"Only {unique_duids} DUIDs found (expected 400+)")
                
        if 'scadavalue' in df.columns:
            # Check for negative generation (except storage)
            negative_gen = df[df['scadavalue'] < 0]
            if not negative_gen.empty:
                result['details']['negative_generation_count'] = len(negative_gen)
                
    def _check_price_data(self, df: pd.DataFrame, result: Dict):
        """Check price-specific data quality"""
        if 'REGIONID' in df.columns:
            regions = df['REGIONID'].unique()
            result['details']['regions'] = list(regions)
            
            expected_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
            missing_regions = set(expected_regions) - set(regions)
            
            if missing_regions:
                result['status'] = 'warning'
                result['issues'].append(f"Missing regions: {missing_regions}")
                
        if 'RRP' in df.columns:
            # Check for extreme prices
            extreme_negative = df[df['RRP'] < -100]
            extreme_positive = df[df['RRP'] > 10000]
            
            if not extreme_negative.empty:
                result['details']['extreme_negative_prices'] = len(extreme_negative)
                
            if not extreme_positive.empty:
                result['details']['extreme_positive_prices'] = len(extreme_positive)
                
    def _check_transmission_data(self, df: pd.DataFrame, result: Dict):
        """Check transmission-specific data quality"""
        if 'interconnectorid' in df.columns:
            interconnectors = df['interconnectorid'].unique()
            result['details']['interconnectors'] = list(interconnectors)
            
            # Check for main interconnectors
            main_interconnectors = [
                'NSW1-QLD1', 'VIC1-NSW1', 'V-SA', 'T-V-MNSP1'
            ]
            
            missing = []
            for ic in main_interconnectors:
                # Check both directions
                if ic not in interconnectors and ic.replace('-', '1-').replace('1-', '-') not in interconnectors:
                    missing.append(ic)
                    
            if missing:
                result['status'] = 'warning'
                result['issues'].append(f"Missing interconnectors: {missing}")
                
        # Check for balanced flows (approximate)
        if 'meteredmwflow' in df.columns and 'interconnectorid' in df.columns:
            # Group by time and sum all flows (should be near zero for closed system)
            time_groups = df.groupby('settlementdate')['meteredmwflow'].sum()
            large_imbalances = time_groups[abs(time_groups) > 1000]
            
            if not large_imbalances.empty:
                result['details']['flow_imbalances'] = len(large_imbalances)
                
    def _check_rooftop_data(self, df: pd.DataFrame, result: Dict):
        """Check rooftop solar-specific data quality"""
        # Check for expected region columns
        expected_cols = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        missing_cols = [col for col in expected_cols if col not in df.columns]
        
        if missing_cols:
            result['status'] = 'warning'
            result['issues'].append(f"Missing region columns: {missing_cols}")
            
        # Check for negative solar (should not happen)
        for col in expected_cols:
            if col in df.columns:
                negative = df[df[col] < 0]
                if not negative.empty:
                    result['issues'].append(f"Negative solar values in {col}")
                    
    def run_complete_check(self) -> Dict[str, Any]:
        """Run integrity check on all data files"""
        logger.info("Starting complete data integrity check")
        
        results = {}
        overall_status = 'healthy'
        total_issues = 0
        
        for name, config in self.files.items():
            logger.info(f"Checking {name}...")
            result = self.check_file_integrity(name, config)
            results[name] = result
            
            # Update overall status
            if result['status'] == 'critical':
                overall_status = 'critical'
            elif result['status'] == 'error' and overall_status != 'critical':
                overall_status = 'error'
            elif result['status'] == 'warning' and overall_status == 'healthy':
                overall_status = 'warning'
                
            total_issues += len(result['issues'])
            
        results['overall_status'] = {
            'status': overall_status,
            'total_issues': total_issues,
            'checked_at': datetime.now()
        }
        
        logger.info(f"Integrity check complete: {overall_status} ({total_issues} issues)")
        
        return results
        
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable report from check results"""
        lines = []
        lines.append("=" * 60)
        lines.append("AEMO DATA INTEGRITY REPORT")
        lines.append(f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}")
        lines.append("=" * 60)
        lines.append("")
        
        overall = results.get('overall_status', {})
        status_emoji = {
            'healthy': '✅',
            'warning': '⚠️',
            'error': '❌',
            'critical': '🚨'
        }
        
        lines.append(f"Overall Status: {status_emoji.get(overall.get('status', 'unknown'))} "
                    f"{overall.get('status', 'unknown').upper()}")
        lines.append(f"Total Issues: {overall.get('total_issues', 0)}")
        lines.append("")
        
        # Individual file reports
        for name, result in results.items():
            if name == 'overall_status':
                continue
                
            lines.append("-" * 40)
            lines.append(f"{name.upper()}")
            lines.append(f"Status: {status_emoji.get(result['status'])} {result['status']}")
            
            details = result.get('details', {})
            if details:
                if 'records' in details:
                    lines.append(f"Records: {details['records']:,}")
                if 'date_range' in details:
                    dr = details['date_range']
                    lines.append(f"Date Range: {dr['start']:%Y-%m-%d} to {dr['end']:%Y-%m-%d} "
                               f"({dr['days']} days)")
                if 'file_size_mb' in details:
                    lines.append(f"File Size: {details['file_size_mb']:.1f} MB")
                    
            if result.get('issues'):
                lines.append("\nIssues:")
                for issue in result['issues']:
                    lines.append(f"  • {issue}")
                    
            if result.get('recommendations'):
                lines.append("\nRecommendations:")
                for rec in result['recommendations']:
                    lines.append(f"  → {rec}")
                    
            lines.append("")
            
        return "\n".join(lines)