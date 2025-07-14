#!/usr/bin/env python3
"""
Simple Status Dashboard for AEMO Data Updater
Real-time monitoring UI with integrity checks and manual controls
"""

import panel as pn
import param
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
from pathlib import Path
import traceback

from ..config import get_config, get_logger
from ..service import AemoDataService

# Configure Panel
pn.extension('tabulator', template='material')

logger = get_logger(__name__)


class SimpleStatusDashboard(param.Parameterized):
    """Simple status dashboard for AEMO Data Updater"""
    
    # Status tracking
    service_status = param.String(default="Stopped")
    last_update_time = param.String(default="Never")
    
    def __init__(self, **params):
        super().__init__(**params)
        self.config = get_config()
        self.service = None
        
        # UI Components
        self.status_indicators = {}
        self.log_messages = []
        
        # Create service instance for monitoring
        try:
            self.service = AemoDataService()
            self.add_log("✅ Service initialized successfully")
        except Exception as e:
            self.add_log(f"❌ Error initializing service: {e}")
        
        # Start status updates
        self.start_status_updates()
    
    def add_log(self, message: str):
        """Add a log message with timestamp"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        self.log_messages.append(log_entry)
        
        # Keep only last 50 messages
        if len(self.log_messages) > 50:
            self.log_messages = self.log_messages[-50:]
        
        logger.info(message)
    
    def get_file_status(self, file_path: Path) -> dict:
        """Get status information for a parquet file"""
        try:
            if not file_path.exists():
                return {
                    "exists": False,
                    "status": "❌ File not found",
                    "records": 0,
                    "size_mb": 0,
                    "latest_date": "N/A"
                }
            
            # Read file info
            file_size = file_path.stat().st_size / (1024*1024)
            df = pd.read_parquet(file_path)
            
            # Find date column
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            latest_date = "N/A"
            age_hours = 999
            
            if date_cols:
                latest_date = pd.to_datetime(df[date_cols[0]]).max()
                age_hours = (datetime.now() - latest_date).total_seconds() / 3600
                latest_date = latest_date.strftime('%Y-%m-%d %H:%M')
            
            # Determine status
            if age_hours < 1:
                status = "🟢 Fresh"
            elif age_hours < 24:
                status = f"🟡 {age_hours:.1f}h old"
            else:
                status = f"🔴 {age_hours:.0f}h old"
            
            return {
                "exists": True,
                "status": status,
                "records": len(df),
                "size_mb": file_size,
                "latest_date": latest_date,
                "age_hours": age_hours
            }
            
        except Exception as e:
            return {
                "exists": True,
                "status": f"❌ Error: {str(e)[:50]}",
                "records": "Error",
                "size_mb": 0,
                "latest_date": "Error"
            }
    
    def create_status_cards(self):
        """Create status cards for all data sources"""
        files_to_check = {
            'Generation': self.config.gen_output_file,
            'Prices': self.config.spot_hist_file,
            'Transmission': self.config.transmission_output_file,
            'Rooftop Solar': self.config.rooftop_solar_file
        }
        
        cards = []
        
        for name, file_path in files_to_check.items():
            status = self.get_file_status(Path(file_path))
            
            # Create card content
            content = f"""
            ## {name}
            **Status:** {status['status']}  
            **Records:** {status['records']:,} records  
            **Size:** {status['size_mb']:.1f} MB  
            **Latest:** {status['latest_date']}  
            **File:** `{Path(file_path).name}`
            """
            
            card = pn.Card(
                pn.pane.Markdown(content),
                title=name,
                width=280,
                height=200,
                margin=10
            )
            cards.append(card)
        
        return pn.GridBox(*cards, ncols=2, width=600)
    
    def run_integrity_check(self):
        """Run data integrity check on all files"""
        self.add_log("🔍 Running data integrity check...")
        
        files_to_check = {
            'Generation': self.config.gen_output_file,
            'Prices': self.config.spot_hist_file,
            'Transmission': self.config.transmission_output_file,
            'Rooftop Solar': self.config.rooftop_solar_file
        }
        
        issues_found = 0
        
        for name, file_path in files_to_check.items():
            try:
                status = self.get_file_status(Path(file_path))
                
                if not status['exists']:
                    self.add_log(f"❌ {name}: File not found")
                    issues_found += 1
                elif status['age_hours'] > 24:
                    self.add_log(f"⚠️  {name}: Data is {status['age_hours']:.0f} hours old")
                    issues_found += 1
                elif status['records'] == 0:
                    self.add_log(f"❌ {name}: No data records")
                    issues_found += 1
                else:
                    self.add_log(f"✅ {name}: {status['records']:,} records, {status['latest_date']}")
                    
            except Exception as e:
                self.add_log(f"❌ {name}: Error checking - {e}")
                issues_found += 1
        
        if issues_found == 0:
            self.add_log("🎉 All data integrity checks passed!")
        else:
            self.add_log(f"⚠️  Found {issues_found} data integrity issues")
        
        return issues_found == 0
    
    def force_update(self):
        """Force a single update cycle"""
        if not self.service:
            self.add_log("❌ No service instance available")
            return
        
        self.add_log("🔄 Running forced update cycle...")
        
        try:
            results = self.service.run_single_update()
            success_count = sum(results.values())
            total_count = len(results)
            
            self.add_log(f"📊 Update complete: {success_count}/{total_count} collectors succeeded")
            
            for name, success in results.items():
                status = "✅" if success else "○"
                self.add_log(f"  {status} {name}")
                
        except Exception as e:
            self.add_log(f"❌ Update failed: {e}")
            self.add_log(f"   {traceback.format_exc()}")
    
    def manual_backfill(self, collector_name: str, days: int = 7):
        """Run manual backfill for specified collector"""
        if not self.service:
            self.add_log("❌ No service instance available")
            return
        
        self.add_log(f"📥 Starting backfill for {collector_name} ({days} days)...")
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            collectors = [collector_name] if collector_name != 'all' else None
            results = self.service.run_backfill(start_date, end_date, collectors)
            
            for name, success in results.items():
                status = "✅ Success" if success else "❌ Failed"
                self.add_log(f"  {status}: {name}")
                
        except Exception as e:
            self.add_log(f"❌ Backfill failed: {e}")
    
    def create_control_panel(self):
        """Create manual control buttons"""
        
        def on_integrity_check(event):
            self.run_integrity_check()
        
        def on_force_update(event):
            self.force_update()
        
        def on_backfill_transmission(event):
            self.manual_backfill('transmission', 7)
        
        def on_clear_log(event):
            self.log_messages.clear()
        
        controls = pn.Column(
            "### Manual Controls",
            pn.Row(
                pn.widgets.Button(
                    name="🔍 Check Integrity", 
                    button_type="success",
                    on_click=on_integrity_check,
                    width=140
                ),
                pn.widgets.Button(
                    name="🔄 Force Update", 
                    button_type="primary",
                    on_click=on_force_update,
                    width=140
                )
            ),
            pn.Row(
                pn.widgets.Button(
                    name="📥 Backfill Transmission", 
                    button_type="warning",
                    on_click=on_backfill_transmission,
                    width=140
                ),
                pn.widgets.Button(
                    name="🗑️ Clear Log", 
                    on_click=on_clear_log,
                    width=140
                )
            ),
            margin=20
        )
        
        return controls
    
    def create_log_panel(self):
        """Create scrollable log panel"""
        log_text = "\n".join(self.log_messages[-30:])  # Show last 30 messages
        
        log_panel = pn.pane.Str(
            log_text,
            styles={
                'font-family': 'monospace',
                'font-size': '12px',
                'background-color': '#2F2F2F',
                'color': '#FFFFFF',
                'padding': '10px',
                'max-height': '300px',
                'overflow-y': 'auto'
            },
            width=600,
            height=300
        )
        
        return pn.Card(log_panel, title="Activity Log", width=620)
    
    def start_status_updates(self):
        """Start background thread for status updates"""
        def update_loop():
            while True:
                try:
                    # Update status periodically
                    time.sleep(10)  # Update every 10 seconds
                except Exception as e:
                    logger.error(f"Status update error: {e}")
                    time.sleep(30)
        
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()
    
    def create_dashboard(self):
        """Create the complete dashboard layout"""
        
        # Title
        title = pn.pane.HTML(
            "<h1>🔌 AEMO Data Updater Status Dashboard</h1>",
            width=800,
            margin=(20, 10)
        )
        
        # Status overview
        status_cards = self.create_status_cards()
        
        # Controls
        controls = self.create_control_panel()
        
        # Log panel
        log_panel = self.create_log_panel()
        
        # Layout
        main_content = pn.Column(
            title,
            pn.Row(status_cards, controls),
            log_panel,
            width=800
        )
        
        return main_content


def run_dashboard(port: int = 5011, host: str = 'localhost'):
    """Run the status dashboard"""
    try:
        dashboard = SimpleStatusDashboard()
        app = dashboard.create_dashboard()
        
        # Create periodic callback to refresh status cards
        def refresh_dashboard():
            try:
                # Refresh status cards
                new_cards = dashboard.create_status_cards()
                app[1][0] = new_cards
                
                # Refresh log panel
                new_log = dashboard.create_log_panel()
                app[2] = new_log
                
            except Exception as e:
                dashboard.add_log(f"Dashboard refresh error: {e}")
        
        # Add periodic callback
        pn.state.add_periodic_callback(refresh_dashboard, period=30000)  # 30 seconds
        
        print(f"🌐 Starting AEMO Updater Status Dashboard")
        print(f"📍 Access at: http://{host}:{port}")
        print(f"🔧 Dashboard Features:")
        print(f"   • Real-time status monitoring")
        print(f"   • Manual integrity checks")
        print(f"   • Force update controls")
        print(f"   • Transmission backfill")
        print(f"   • Activity logging")
        
        # Serve the app
        app.show(port=port, host=host, open=True, autoreload=False)
        
    except Exception as e:
        print(f"❌ Dashboard startup failed: {e}")
        print(f"   Error details: {traceback.format_exc()}")


def main():
    """Main entry point"""
    run_dashboard()


if __name__ == "__main__":
    main()