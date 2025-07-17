#!/usr/bin/env python3
"""
Standalone AEMO Data Updater Status Dashboard
Real-time monitoring without complex service dependencies
"""

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_updater.config import get_config

# Configure Panel for Material Design
pn.extension('tabulator', template='material')
pn.config.sizing_mode = 'stretch_width'


class AEMOStatusDashboard:
    """Simple file-based status dashboard"""
    
    def __init__(self):
        self.config = get_config()
        self.log_messages = []
        
        # Data sources to monitor
        self.data_sources = {
            'Generation': {
                'file': self.config.gen_output_file,
                'description': 'SCADA generation data by DUID',
                'update_freq': '5 minutes'
            },
            'Prices': {
                'file': self.config.spot_hist_file,
                'description': 'Regional electricity spot prices',
                'update_freq': '5 minutes'
            },
            'Transmission': {
                'file': self.config.transmission_output_file,
                'description': 'Interconnector flow data',
                'update_freq': '5 minutes'
            },
            'Rooftop Solar': {
                'file': self.config.rooftop_solar_file,
                'description': 'Distributed PV generation (5-min converted)',
                'update_freq': '30 minutes'
            }
        }
    
    def add_log(self, message: str):
        """Add timestamped log message"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_messages.append(f"[{timestamp}] {message}")
        if len(self.log_messages) > 30:
            self.log_messages = self.log_messages[-30:]
    
    def get_file_status(self, file_path: Path) -> dict:
        """Analyze a parquet file and return status info"""
        try:
            if not file_path.exists():
                return {
                    'status': '🔴',
                    'status_text': 'File not found',
                    'records': 0,
                    'size_mb': 0,
                    'latest_data': 'N/A',
                    'age_hours': 999,
                    'health': 'error'
                }
            
            # Get file info
            file_size = file_path.stat().st_size / (1024*1024)
            df = pd.read_parquet(file_path)
            
            # Find date column or check index
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            
            if date_cols:
                latest_dt = pd.to_datetime(df[date_cols[0]]).max()
                age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
                latest_str = latest_dt.strftime('%d-%b %H:%M')
            elif hasattr(df.index, 'name') and df.index.name and ('date' in str(df.index.name).lower() or 'time' in str(df.index.name).lower()):
                # Date is in the index
                latest_dt = pd.to_datetime(df.index).max()
                age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
                latest_str = latest_dt.strftime('%d-%b %H:%M')
            else:
                latest_str = 'No date column'
                age_hours = 999
            
            # Determine health status (30 minute threshold for alerts)
            if age_hours < 0.5:  # Less than 30 minutes
                status, health = '🟢', 'excellent'
                status_text = 'Fresh data'
            elif age_hours < 1:
                status, health = '🟡', 'good'
                status_text = f'{int(age_hours * 60)}min old'
            elif age_hours < 6:
                status, health = '🟠', 'stale'
                status_text = f'{age_hours:.1f}h old'
            else:
                status, health = '🔴', 'very_stale'
                status_text = f'{age_hours:.0f}h old'
            
            return {
                'status': status,
                'status_text': status_text,
                'records': len(df),
                'size_mb': file_size,
                'latest_data': latest_str,
                'age_hours': age_hours,
                'health': health
            }
            
        except Exception as e:
            return {
                'status': '❌',
                'status_text': f'Error: {str(e)[:30]}...',
                'records': 0,
                'size_mb': 0,
                'latest_data': 'Error',
                'age_hours': 999,
                'health': 'error'
            }
    
    def create_status_table(self) -> pn.widgets.Tabulator:
        """Create a compact status table for all data sources"""
        data = []
        
        for name, info in self.data_sources.items():
            status = self.get_file_status(Path(info['file']))
            
            # Convert status to check/cross
            if status['health'] in ['excellent', 'good']:
                status_icon = '✅'
            elif status['health'] in ['stale', 'very_stale']:
                status_icon = '⚠️'
            else:
                status_icon = '❌'
            
            data.append({
                'Source': name,
                'Status': status_icon,
                'Records': f"{status['records']:,}",
                'Size (MB)': f"{status['size_mb']:.1f}",
                'Latest Data': status['latest_data'],
                'Age': status['status_text'],
                'Update Freq': info['update_freq']
            })
        
        df = pd.DataFrame(data)
        
        # Create tabulator with custom formatting
        table = pn.widgets.Tabulator(
            df,
            height=200,
            width=720,
            sizing_mode='stretch_width',
            show_index=False,
            layout='fit_columns',
            header_align='center',
            text_align={
                'Status': 'center',
                'Records': 'right',
                'Size (MB)': 'right',
                'Update Freq': 'center'
            }
        )
        
        return table
    
    def create_summary_stats(self) -> pn.Row:
        """Create overall system summary"""
        total_files = len(self.data_sources)
        healthy_files = 0
        total_records = 0
        total_size = 0
        
        for name, info in self.data_sources.items():
            status = self.get_file_status(Path(info['file']))
            if status['health'] in ['excellent', 'good']:
                healthy_files += 1
            total_records += status['records']
            total_size += status['size_mb']
        
        health_pct = (healthy_files / total_files) * 100 if total_files > 0 else 0
        
        # Overall health indicator with current time
        current_time = datetime.now().strftime('%H:%M')
        if health_pct >= 100:
            overall_status = f"🟢 All Systems Operational at {current_time}"
            color = "#4CAF50"
        elif health_pct >= 75:
            overall_status = f"🟡 Mostly Operational at {current_time}" 
            color = "#FFC107"
        elif health_pct >= 50:
            overall_status = f"🟠 Some Issues at {current_time}"
            color = "#FF9800"
        else:
            overall_status = f"🔴 Multiple Issues at {current_time}"
            color = "#F44336"
        
        # Create individual stat panels
        stat1 = pn.Column(
            pn.pane.HTML(f"<h2 style='margin: 0;'>{healthy_files}/{total_files}</h2>", align='center'),
            pn.pane.HTML("<p style='margin: 0; color: #666;'>Healthy Sources</p>", align='center'),
            align='center'
        )
        
        stat2 = pn.Column(
            pn.pane.HTML(f"<h2 style='margin: 0;'>{total_records:,}</h2>", align='center'),
            pn.pane.HTML("<p style='margin: 0; color: #666;'>Total Records</p>", align='center'),
            align='center'
        )
        
        stat3 = pn.Column(
            pn.pane.HTML(f"<h2 style='margin: 0;'>{total_size:.1f} MB</h2>", align='center'),
            pn.pane.HTML("<p style='margin: 0; color: #666;'>Total Data</p>", align='center'),
            align='center'
        )
        
        summary = pn.Card(
            pn.Row(
                stat1, stat2, stat3,
                align='center',
                sizing_mode='stretch_width'
            ),
            title=overall_status,
            title_css_classes=['text-center'],
            width=720,
            height=120,
            margin=10,
            header_color=color
        )
        
        return summary
    
    def find_missing_intervals(self, df, name, expected_interval_minutes=5):
        """Find exact missing time intervals in the last 24 hours"""
        now = datetime.now()
        cutoff_24h = now - timedelta(hours=24)
        
        # Generate expected time series for last 24 hours
        expected_times = pd.date_range(
            start=cutoff_24h.replace(second=0, microsecond=0),
            end=now.replace(second=0, microsecond=0),
            freq=f'{expected_interval_minutes}min'
        )
        
        if len(df) == 0:
            return list(expected_times), f"No data available - missing all {len(expected_times)} intervals"
        
        # Find date column
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if not date_cols:
            if hasattr(df.index, 'name') and 'date' in str(df.index.name).lower():
                actual_times = pd.to_datetime(df.index)
            else:
                return [], "No date column found"
        else:
            actual_times = pd.to_datetime(df[date_cols[0]])
        
        # Filter to recent data only
        recent_actual = actual_times[actual_times >= cutoff_24h]
        recent_actual = recent_actual.round(f'{expected_interval_minutes}min')  # Round to nearest interval
        
        # Find missing intervals
        missing_times = expected_times.difference(recent_actual)
        
        if len(missing_times) == 0:
            return [], None
        
        # Group consecutive missing periods
        missing_list = missing_times.sort_values().tolist()
        if len(missing_list) > 10:  # If too many, summarize
            gap_summary = f"Missing {len(missing_list)} intervals ({missing_list[0].strftime('%m-%d %H:%M')} to {missing_list[-1].strftime('%m-%d %H:%M')})"
        else:
            gap_summary = f"Missing {len(missing_list)} intervals: " + ", ".join([t.strftime('%m-%d %H:%M') for t in missing_list[:5]])
            if len(missing_list) > 5:
                gap_summary += f" ... and {len(missing_list)-5} more"
        
        return missing_list, gap_summary
        
    def check_data_gaps(self, df, name, expected_interval_minutes=5):
        """Check for gaps in time series data with focus on recent periods"""
        issues = []
        
        # Check for missing intervals in last 24 hours (priority)
        missing_intervals, gap_summary = self.find_missing_intervals(df, name, expected_interval_minutes)
        
        if gap_summary:
            issues.append(f"🚨 RECENT: {gap_summary}")
            # Store missing intervals for potential repair
            if not hasattr(self, 'missing_data'):
                self.missing_data = {}
            self.missing_data[name] = missing_intervals
        
        # Quick check for older gaps (lower priority)
        if len(df) > 0:
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            if date_cols:
                dates = pd.to_datetime(df[date_cols[0]])
            elif hasattr(df.index, 'name') and 'date' in str(df.index.name).lower():
                dates = pd.to_datetime(df.index)
            else:
                return issues
                
            # Check for large historical gaps (>4 hours)
            dates_sorted = dates.sort_values().drop_duplicates()
            if len(dates_sorted) > 1:
                time_diffs = dates_sorted.diff()
                large_gaps = time_diffs[time_diffs > pd.Timedelta(hours=4)]
                if len(large_gaps) > 0:
                    max_gap_hours = large_gaps.max().total_seconds() / 3600
                    issues.append(f"📊 Historical: {len(large_gaps)} gaps >4h, largest: {max_gap_hours:.0f}h")
        
        return issues
    
    def run_integrity_check(self):
        """Comprehensive data integrity check"""
        self.add_log("🔍 Running comprehensive data integrity check...")
        
        total_issues = 0
        
        for name, info in self.data_sources.items():
            self.add_log(f"📊 Checking {name}...")
            
            # Basic file status
            status = self.get_file_status(Path(info['file']))
            
            if status['health'] == 'error':
                self.add_log(f"❌ {name}: {status['status_text']}")
                total_issues += 1
                continue
            
            # Age check
            if status['age_hours'] > 0.5:  # Over 30 minutes old
                age_minutes = int(status['age_hours'] * 60)
                self.add_log(f"⚠️  {name}: Data is {age_minutes} minutes old (threshold: 30 min)")
                total_issues += 1
            
            # Detailed gap analysis
            try:
                file_path = Path(info['file'])
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    
                    # Determine expected interval
                    expected_interval = 30 if name == 'Rooftop Solar' else 5
                    
                    # Check for gaps
                    gap_issues = self.check_data_gaps(df, name, expected_interval)
                    
                    if gap_issues:
                        for issue in gap_issues:
                            self.add_log(f"⚠️  {name}: {issue}")
                            total_issues += 1
                    else:
                        self.add_log(f"✅ {name}: {status['records']:,} records, no gaps detected")
                        
                    # Additional checks for specific data types
                    if name == 'Generation' and len(df) > 0:
                        # Check DUID count per timestamp
                        if 'duid' in df.columns and 'settlementdate' in df.columns:
                            duid_counts = df.groupby('settlementdate')['duid'].count()
                            low_counts = duid_counts[duid_counts < 400]  # Should have ~470 DUIDs
                            if len(low_counts) > 0:
                                self.add_log(f"⚠️  {name}: {len(low_counts)} timestamps with < 400 DUIDs")
                                total_issues += 1
                    
                    elif name == 'Prices' and len(df) > 0:
                        # Check region count per timestamp
                        if 'REGIONID' in df.columns:
                            if hasattr(df.index, 'name') and 'date' in str(df.index.name).lower():
                                region_counts = df.groupby(df.index)['REGIONID'].count()
                            else:
                                region_counts = df.groupby('SETTLEMENTDATE')['REGIONID'].count()
                            low_counts = region_counts[region_counts < 5]  # Should have 5 regions
                            if len(low_counts) > 0:
                                self.add_log(f"⚠️  {name}: {len(low_counts)} timestamps with < 5 regions")
                                total_issues += 1
                
            except Exception as e:
                self.add_log(f"❌ {name}: Error analyzing data - {str(e)[:50]}...")
                total_issues += 1
        
        # Summary
        if total_issues == 0:
            self.add_log("🎉 All integrity checks passed!")
        else:
            self.add_log(f"⚠️  Found {total_issues} integrity issues total")
            
            # Check if we have recent missing data that can be repaired
            if hasattr(self, 'missing_data') and self.missing_data:
                recent_missing = sum(1 for name, intervals in self.missing_data.items() if intervals)
                if recent_missing > 0:
                    self.add_log(f"💡 {recent_missing} data sources have recent gaps that can be repaired")
                    self.add_log("   Use 'Fix Missing Data' button to repair recent gaps")
        
        return total_issues == 0
    
    def repair_missing_data(self):
        """Repair missing data for recent periods"""
        if not hasattr(self, 'missing_data') or not self.missing_data:
            self.add_log("❌ No missing data identified. Run integrity check first.")
            return
        
        self.add_log("🔧 Starting data repair process...")
        
        repaired_count = 0
        for name, missing_intervals in self.missing_data.items():
            if not missing_intervals:
                continue
                
            self.add_log(f"🔄 Repairing {name}: {len(missing_intervals)} missing intervals")
            
            try:
                # Call the real repair method for this data source
                success = self.repair_data_source(name, missing_intervals)
                
                if success:
                    self.add_log(f"✅ {name}: Successfully repaired {len(missing_intervals)} intervals")
                    repaired_count += 1
                else:
                    self.add_log(f"❌ {name}: Repair failed - data may not be available")
                    
            except Exception as e:
                self.add_log(f"❌ {name}: Repair error - {str(e)[:50]}...")
        
        if repaired_count > 0:
            self.add_log(f"🎉 Repair complete! Fixed {repaired_count} data sources")
            self.add_log("💡 Run integrity check again to verify repairs")
        else:
            self.add_log("⚠️  No data could be repaired. Check network or data availability")
        
        # Clear missing data tracker
        self.missing_data = {}
    
    def repair_data_source(self, name, missing_intervals):
        """Repair missing data for a specific data source"""
        try:
            # Import collectors
            import sys
            sys.path.append('src')
            
            if name == 'Prices':
                from aemo_updater.collectors.price_collector import PriceCollector
                collector = PriceCollector()
                return collector.backfill_missing_data(missing_intervals)
            
            elif name == 'Generation':
                from aemo_updater.collectors.generation_collector import GenerationCollector
                collector = GenerationCollector()
                # Add backfill method if it exists
                if hasattr(collector, 'backfill_missing_data'):
                    return collector.backfill_missing_data(missing_intervals)
                else:
                    self.add_log(f"⚠️  {name}: Backfill not yet implemented")
                    return False
            
            elif name == 'Transmission':
                # Transmission backfill would be implemented similarly
                self.add_log(f"⚠️  {name}: Backfill not yet implemented")
                return False
                
            elif name == 'Rooftop Solar':
                # Rooftop solar backfill would be implemented similarly
                self.add_log(f"⚠️  {name}: Backfill not yet implemented")
                return False
            
            else:
                self.add_log(f"❌ {name}: Unknown data source")
                return False
                
        except ImportError as e:
            self.add_log(f"❌ {name}: Import error - {str(e)[:50]}...")
            return False
        except Exception as e:
            self.add_log(f"❌ {name}: Repair error - {str(e)[:50]}...")
            return False
    
    def create_controls(self) -> pn.Row:
        """Create manual control buttons"""
        
        def on_refresh(event):
            self.add_log("🔄 Manual refresh triggered")
            if hasattr(self, 'refresh_dashboard'):
                self.refresh_dashboard()
        
        def on_integrity_check(event):
            self.run_integrity_check()
            # Trigger a refresh to show the log updates
            if hasattr(self, 'refresh_dashboard'):
                self.refresh_dashboard()
        
        def on_repair_data(event):
            self.repair_missing_data()
            # Trigger a refresh to show the log updates
            if hasattr(self, 'refresh_dashboard'):
                self.refresh_dashboard()
        
        def on_clear_log(event):
            self.log_messages.clear()
            self.add_log("📝 Log cleared")
        
        # Create refresh button separately so we can add periodic callback
        self.refresh_button = pn.widgets.Button(
            name="🔄 Refresh Status", 
            button_type="primary",
            on_click=on_refresh,
            width=140
        )
        
        # First row of controls
        controls_row1 = pn.Row(
            self.refresh_button,
            pn.widgets.Button(
                name="🔍 Check Integrity", 
                button_type="success",
                on_click=on_integrity_check,
                width=140
            ),
            margin=(5, 0)
        )
        
        # Second row of controls
        controls_row2 = pn.Row(
            pn.widgets.Button(
                name="🔧 Fix Missing Data", 
                button_type="light",
                on_click=on_repair_data,
                width=140
            ),
            pn.widgets.Button(
                name="🗑️ Clear Log", 
                on_click=on_clear_log,
                width=140
            ),
            margin=(5, 0)
        )
        
        controls = pn.Column(controls_row1, controls_row2, margin=(10, 0))
        
        return controls
    
    def create_log_viewer(self) -> pn.Card:
        """Create activity log viewer"""
        return pn.Card(
            pn.pane.Str(
                "\n".join(self.log_messages[-20:]),
                styles={
                    'font-family': 'monospace',
                    'font-size': '12px',
                    'background-color': '#1E1E1E',
                    'color': '#FFFFFF',
                    'padding': '15px',
                    'border-radius': '5px',
                    'height': '200px',
                    'overflow-y': 'auto'
                }
            ),
            title="Activity Log",
            width=720,
            height=250,
            margin=10
        )
    
    def create_dashboard(self) -> pn.template.MaterialTemplate:
        """Create the complete dashboard"""
        
        # Initialize log
        self.add_log("🚀 AEMO Data Updater Dashboard started")
        self.add_log("📊 Monitoring 4 data sources...")
        
        # Create layout components
        summary = self.create_summary_stats()
        status_table = self.create_status_table()
        log_viewer = self.create_log_viewer()
        controls = self.create_controls()
        
        # Main content
        main_content = pn.Column(
            pn.pane.HTML("<h1>⚡ AEMO Data Updater Status Dashboard</h1>"),
            summary,
            pn.Card(status_table, title="Data Source Status", width=720, margin=10),
            log_viewer,
            controls,
            sizing_mode='stretch_width'
        )
        
        # Refresh function with logging
        def refresh_dashboard():
            self.add_log("🔄 Auto-refresh: Updating status...")
            
            # Update summary
            main_content[1] = self.create_summary_stats()
            
            # Update status table
            new_table = self.create_status_table()
            main_content[2] = pn.Card(new_table, title="Data Source Status", width=720, margin=10)
            
            # Update log
            main_content[3] = self.create_log_viewer()
        
        # Store refresh function for manual refresh button
        self.refresh_dashboard = refresh_dashboard
        
        return main_content
    
    def setup_periodic_refresh(self):
        """Set up periodic refresh after server starts"""
        def periodic_refresh():
            self.add_log("🔄 Auto-refresh: Updating status...")
            if hasattr(self, 'refresh_dashboard'):
                self.refresh_dashboard()
        
        # Add periodic callback that runs every 30 seconds
        self.periodic_callback = pn.state.add_periodic_callback(
            periodic_refresh, 
            period=30000,  # 30 seconds in milliseconds
            start=True
        )
        self.add_log("✅ Auto-refresh enabled (every 30 seconds)")


def main():
    """Run the dashboard"""
    try:
        dashboard = AEMOStatusDashboard()
        app = dashboard.create_dashboard()
        
        print("🌐 AEMO Data Updater Status Dashboard")
        print("=" * 50)
        print("📍 Dashboard will open in your browser")
        print("🔧 Features:")
        print("   • Compact table view of all data sources")
        print("   • Real-time file monitoring")
        print("   • Data integrity checking")  
        print("   • Activity logging with auto-refresh tracking")
        print("📊 Monitoring files:")
        
        for name, info in dashboard.data_sources.items():
            print(f"   • {name}: {Path(info['file']).name}")
        
        print("\n🔄 Auto-refresh every 30 seconds")
        print("🛑 Press Ctrl+C to stop")
        
        # Create a function that returns the app and sets up periodic refresh
        def create_app():
            # Set up periodic refresh after server starts
            pn.state.onload(dashboard.setup_periodic_refresh)
            return app
        
        # Serve the dashboard with the setup function
        pn.serve(create_app, port=5011, show=True, autoreload=False)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()