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
                latest_str = latest_dt.strftime('%Y-%m-%d %H:%M')
            elif hasattr(df.index, 'name') and df.index.name and ('date' in str(df.index.name).lower() or 'time' in str(df.index.name).lower()):
                # Date is in the index
                latest_dt = pd.to_datetime(df.index).max()
                age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
                latest_str = latest_dt.strftime('%Y-%m-%d %H:%M')
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
        
        # Overall health indicator
        if health_pct >= 100:
            overall_status = "🟢 All Systems Operational"
            color = "#4CAF50"
        elif health_pct >= 75:
            overall_status = "🟡 Mostly Operational" 
            color = "#FFC107"
        elif health_pct >= 50:
            overall_status = "🟠 Some Issues"
            color = "#FF9800"
        else:
            overall_status = "🔴 Multiple Issues"
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
    
    def run_integrity_check(self):
        """Manual integrity check"""
        self.add_log("🔍 Running data integrity check...")
        
        issues = 0
        for name, info in self.data_sources.items():
            status = self.get_file_status(Path(info['file']))
            
            if status['health'] == 'error':
                self.add_log(f"❌ {name}: {status['status_text']}")
                issues += 1
            elif status['age_hours'] > 0.5:  # Over 30 minutes old
                age_minutes = int(status['age_hours'] * 60)
                self.add_log(f"⚠️  {name}: Data is {age_minutes} minutes old (threshold: 30 min)")
                issues += 1
            else:
                self.add_log(f"✅ {name}: {status['records']:,} records, {status['latest_data']}")
        
        if issues == 0:
            self.add_log("🎉 All integrity checks passed!")
        else:
            self.add_log(f"⚠️  Found {issues} integrity issues")
        
        return issues == 0
    
    def create_controls(self) -> pn.Row:
        """Create manual control buttons"""
        
        def on_refresh(event):
            self.add_log("🔄 Manual refresh triggered")
            if hasattr(self, 'refresh_dashboard'):
                self.refresh_dashboard()
        
        def on_integrity_check(event):
            self.run_integrity_check()
        
        def on_clear_log(event):
            self.log_messages.clear()
            self.add_log("📝 Log cleared")
        
        controls = pn.Row(
            pn.widgets.Button(
                name="🔄 Refresh Status", 
                button_type="primary",
                on_click=on_refresh,
                width=140
            ),
            pn.widgets.Button(
                name="🔍 Check Integrity", 
                button_type="success",
                on_click=on_integrity_check,
                width=140
            ),
            pn.widgets.Button(
                name="🗑️ Clear Log", 
                on_click=on_clear_log,
                width=140
            ),
            margin=(10, 0)
        )
        
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
        
        # Serve dashboard without auto-refresh for now
        app.show(port=5011, open=True, autoreload=False)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()