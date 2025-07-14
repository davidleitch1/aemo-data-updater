"""
Status monitoring dashboard for AEMO Data Updater
Provides real-time status, controls, and data integrity checks
"""

import panel as pn
import param
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from ..config import PARQUET_FILES, STATUS_UI_PORT, STATUS_UI_HOST, QUALITY_THRESHOLDS, get_config
from ..service import UpdaterService
from ..integrity import DataIntegrityChecker
from ..alerts import AlertManager, Alert, AlertSeverity, AlertChannel

# Configure Panel
pn.extension('tabulator', template='material')
pn.config.theme = 'dark'

logger = logging.getLogger('aemo_updater.ui')


class StatusDashboard(param.Parameterized):
    """Main status dashboard for AEMO Data Updater"""
    
    # Status indicators
    service_running = param.Boolean(default=False)
    last_update = param.String(default='Never')
    
    # Control parameters
    backfill_source = param.Selector(
        default='All',
        objects=['All', 'Generation', 'Price', 'Transmission', 'Rooftop'],
        doc="Data source to backfill"
    )
    backfill_start = param.Date(
        default=(datetime.now() - timedelta(days=7)).date(),
        doc="Backfill start date"
    )
    backfill_end = param.Date(
        default=datetime.now().date(),
        doc="Backfill end date"
    )
    
    def __init__(self, **params):
        super().__init__(**params)
        self.service = UpdaterService()
        self.integrity_checker = DataIntegrityChecker()
        
        # Initialize alert manager
        config = get_config()
        self.alert_manager = AlertManager(vars(config))
        self.last_alert_check = datetime.now()
        
        # UI components
        self.status_indicators = {}
        self.log_pane = pn.pane.Str(
            "Waiting for updates...\n",
            height=200,
            styles={'font-family': 'monospace', 'font-size': '12px'}
        )
        
        # Start periodic updates
        pn.state.add_periodic_callback(self.update_status, period=5000)  # 5 seconds
        pn.state.add_periodic_callback(self.check_alerts, period=60000)  # 1 minute
        
    def create_status_card(self, name: str) -> pn.Card:
        """Create status card for a data source"""
        config = PARQUET_FILES[name]
        
        # Status indicator (●)
        indicator = pn.pane.HTML(
            "<div style='font-size: 24px;'>●</div>",
            width=30,
            height=30,
            margin=(5, 10)
        )
        self.status_indicators[name] = indicator
        
        # Info text
        info = pn.pane.Markdown(
            f"**{config['description']}**\n\n"
            f"File: `{config['path'].name}`\n\n"
            f"Update: Every {config['update_interval']/60:.0f} minutes\n\n"
            f"Status: Checking...",
            width=250
        )
        
        # Create card
        card = pn.Card(
            pn.Row(indicator, info),
            title=name.title(),
            width=320,
            height=180,
            margin=10
        )
        
        return card
        
    def check_alerts(self):
        """Check for alerts and send notifications"""
        asyncio.create_task(self._check_alerts_async())
        
    async def _check_alerts_async(self):
        """Async alert checking"""
        try:
            # Get current statuses
            statuses = self.service.get_all_status()
            
            # Get type-specific thresholds
            thresholds = QUALITY_THRESHOLDS.get('max_age_minutes_by_type', {})
            
            # Check for alerts
            alerts = await self.alert_manager.check_data_freshness(statuses, thresholds)
            
            # Send alerts
            for alert in alerts:
                self.add_log(f"ALERT: {alert.title}")
                await self.alert_manager.send_alert(alert)
                
        except Exception as e:
            logger.error(f"Alert check failed: {e}")
            self.add_log(f"Alert check error: {e}")
    
    def update_status(self):
        """Update status indicators and information"""
        statuses = self.service.get_all_status()
        
        for name, status in statuses.items():
            if name not in self.status_indicators:
                continue
                
            indicator = self.status_indicators[name]
            file_info = status.get('file_info', {})
            
            # Determine status color
            if not file_info.get('exists'):
                color = 'red'
                status_text = 'No data file'
            elif status.get('last_success'):
                # Check data freshness
                if file_info.get('modified'):
                    age = (datetime.now() - file_info['modified']).total_seconds() / 60
                    # Use type-specific threshold if available
                    max_age = QUALITY_THRESHOLDS.get('max_age_minutes_by_type', {}).get(
                        name, QUALITY_THRESHOLDS['max_age_minutes']
                    )
                    if age > max_age:
                        color = 'orange'
                        status_text = f'Stale ({age:.0f}m old)'
                    else:
                        color = 'green'
                        status_text = 'Up to date'
                else:
                    color = 'orange'
                    status_text = 'Unknown freshness'
            else:
                color = 'red'
                status_text = f"Error: {status.get('last_error', 'Unknown')}"
                
            # Update indicator
            indicator.object = f"<div style='font-size: 24px; color: {color};'>●</div>"
            
            # Update card info
            card = indicator.parent.parent
            info_pane = card[0][1]
            
            info_text = f"**{PARQUET_FILES[name]['description']}**\n\n"
            
            if file_info.get('exists'):
                info_text += f"Size: {file_info.get('size_mb', 0):.1f} MB\n"
                info_text += f"Records: {file_info.get('records', 'Unknown'):,}\n"
                
                if 'date_range' in file_info:
                    start = file_info['date_range']['start']
                    end = file_info['date_range']['end']
                    if pd.notna(start) and pd.notna(end):
                        info_text += f"Range: {start:%Y-%m-%d} to {end:%Y-%m-%d}\n"
                        
                info_text += f"\nStatus: **{status_text}**"
            else:
                info_text += f"\nStatus: **{status_text}**"
                
            info_pane.object = info_text
            
    @param.depends('backfill_source', 'backfill_start', 'backfill_end')
    def backfill_controls(self):
        """Backfill control panel"""
        return pn.Column(
            "### Backfill Controls",
            pn.Row(
                pn.widgets.Select.from_param(self.param.backfill_source, width=150),
                pn.widgets.DatePicker.from_param(self.param.backfill_start, width=120),
                pn.widgets.DatePicker.from_param(self.param.backfill_end, width=120),
            ),
            pn.Row(
                pn.widgets.Button(
                    name='Run Backfill',
                    button_type='primary',
                    on_click=self.run_backfill,
                    width=150
                ),
                pn.widgets.Button(
                    name='Check Integrity',
                    button_type='warning',
                    on_click=self.run_integrity_check,
                    width=150
                ),
            ),
            margin=(20, 10)
        )
        
    def run_backfill(self, event):
        """Handle backfill button click"""
        self.add_log(f"Starting backfill for {self.backfill_source} "
                    f"from {self.backfill_start} to {self.backfill_end}")
        
        # Run backfill asynchronously
        asyncio.create_task(self._run_backfill_async())
        
    async def _run_backfill_async(self):
        """Run backfill asynchronously"""
        try:
            sources = ['generation', 'price', 'transmission', 'rooftop'] \
                     if self.backfill_source == 'All' else [self.backfill_source.lower()]
                     
            for source in sources:
                self.add_log(f"Backfilling {source}...")
                success = await self.service.backfill_source(
                    source,
                    self.backfill_start,
                    self.backfill_end
                )
                
                if success:
                    self.add_log(f"✓ {source} backfill complete")
                else:
                    self.add_log(f"✗ {source} backfill failed")
                    
        except Exception as e:
            self.add_log(f"Backfill error: {e}")
            
    def run_integrity_check(self, event):
        """Handle integrity check button click"""
        self.add_log("Running data integrity check...")
        
        try:
            report = self.integrity_checker.run_complete_check()
            
            # Display summary
            self.add_log("\n=== Data Integrity Report ===")
            
            for source, info in report.items():
                if source == 'overall_status':
                    continue
                    
                status = '✓' if info['status'] == 'healthy' else '✗'
                self.add_log(f"\n{status} {source.upper()}")
                
                if info.get('issues'):
                    for issue in info['issues']:
                        self.add_log(f"  - {issue}")
                        
                if info.get('recommendations'):
                    for rec in info['recommendations']:
                        self.add_log(f"  → {rec}")
                        
            self.add_log("\n=== Check Complete ===")
            
        except Exception as e:
            self.add_log(f"Integrity check error: {e}")
            
    def add_log(self, message: str):
        """Add message to log display"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        current_log = self.log_pane.object
        
        # Keep last 50 lines
        lines = current_log.split('\n')
        if len(lines) > 50:
            lines = lines[-50:]
            
        lines.append(f"[{timestamp}] {message}")
        self.log_pane.object = '\n'.join(lines)
        
    def control_panel(self):
        """Service control panel"""
        return pn.Column(
            "### Service Control",
            pn.Row(
                pn.widgets.Button(
                    name='Start Service' if not self.service_running else 'Stop Service',
                    button_type='success' if not self.service_running else 'danger',
                    on_click=self.toggle_service,
                    width=120
                ),
                pn.widgets.Button(
                    name='Force Update',
                    button_type='primary',
                    on_click=self.force_update,
                    width=120
                ),
                pn.widgets.Button(
                    name='Clear Logs',
                    on_click=lambda e: setattr(self.log_pane, 'object', ''),
                    width=120
                ),
            ),
            pn.Row(
                pn.widgets.Button(
                    name='Test Alerts',
                    button_type='warning',
                    on_click=self.test_alerts,
                    width=120
                ),
                pn.widgets.Button(
                    name='Check Alerts',
                    button_type='info',
                    on_click=lambda e: self.check_alerts(),
                    width=120
                ),
            ),
            margin=(20, 10)
        )
        
    def toggle_service(self, event):
        """Start/stop the update service"""
        if not self.service_running:
            self.add_log("Starting update service...")
            asyncio.create_task(self.service.start())
            self.service_running = True
            event.obj.name = 'Stop Service'
            event.obj.button_type = 'danger'
        else:
            self.add_log("Stopping update service...")
            asyncio.create_task(self.service.stop())
            self.service_running = False
            event.obj.name = 'Start Service'
            event.obj.button_type = 'success'
            
    def force_update(self, event):
        """Force immediate update cycle"""
        self.add_log("Running forced update cycle...")
        asyncio.create_task(self.service.run_once())
        
    def test_alerts(self, event):
        """Test alert channels"""
        self.add_log("Testing alert channels...")
        asyncio.create_task(self._test_alerts_async())
        
    async def _test_alerts_async(self):
        """Test alerts asynchronously"""
        try:
            # Test channel connections
            results = self.alert_manager.test_channels()
            
            for channel, success in results.items():
                if success:
                    self.add_log(f"✓ {channel.upper()} alerts: Connected")
                else:
                    self.add_log(f"✗ {channel.upper()} alerts: Failed")
                    
            # Send test alert if any channel works
            if any(results.values()):
                test_alert = Alert(
                    title="Test Alert",
                    message="This is a test alert from AEMO Data Updater status dashboard.",
                    severity=AlertSeverity.INFO,
                    source="status_dashboard",
                    metadata={"test": True}
                )
                
                success = await self.alert_manager.send_alert(
                    test_alert, 
                    AlertChannel.BOTH
                )
                
                if success:
                    self.add_log("✓ Test alert sent successfully")
                else:
                    self.add_log("✗ Failed to send test alert")
            else:
                self.add_log("No alert channels configured")
                
        except Exception as e:
            self.add_log(f"Alert test error: {e}")
        
    def create_layout(self):
        """Create the complete dashboard layout"""
        # Status cards grid
        status_grid = pn.GridBox(
            *[self.create_status_card(name) for name in PARQUET_FILES.keys()],
            ncols=2,
            width=700
        )
        
        # Control panels
        controls = pn.Column(
            self.control_panel(),
            pn.layout.Divider(),
            self.backfill_controls(),
            width=400
        )
        
        # Log viewer
        log_card = pn.Card(
            self.log_pane,
            title="Activity Log",
            width=1120,
            height=250,
            margin=10
        )
        
        # Main layout
        return pn.template.MaterialTemplate(
            title="AEMO Data Updater Status",
            sidebar=[controls],
            main=[
                pn.Row(status_grid),
                log_card
            ],
        )


def main():
    """Main entry point for status dashboard"""
    dashboard = StatusDashboard()
    
    # Create and serve the app
    app = dashboard.create_layout()
    
    print(f"Starting AEMO Updater Status Dashboard on http://{STATUS_UI_HOST}:{STATUS_UI_PORT}")
    app.show(port=STATUS_UI_PORT, open=False)
    

if __name__ == "__main__":
    main()