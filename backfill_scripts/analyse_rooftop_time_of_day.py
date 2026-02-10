#!/usr/bin/env python3
"""
Rooftop Solar Time-of-Day Comparison

Compares rooftop solar output by hour of day across three periods:
- Last 30 days
- Same period last year
- Same period 2 years ago

Usage:
    python analyse_rooftop_time_of_day.py

Then open browser to http://localhost:5021
"""

import pandas as pd
import panel as pn
import hvplot.pandas
from datetime import datetime, timedelta

# Enable Panel extension
pn.extension()

# Load rooftop30 data
print("Loading rooftop30.parquet...")
df = pd.read_parquet('/Volumes/davidleitch/aemo_production/data/rooftop30.parquet')
print(f"Loaded {len(df):,} records from {df['settlementdate'].min()} to {df['settlementdate'].max()}")

# Create region selector
region_selector = pn.widgets.Select(
    name='Region',
    options=['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
    value='NSW1'
)

@pn.depends(region_selector.param.value)
def create_time_of_day_plot(region):
    """Create time-of-day comparison plot for selected region"""

    # Calculate date ranges
    end_date = datetime.now()
    start_current = end_date - timedelta(days=30)

    start_1yr = start_current - timedelta(days=365)
    end_1yr = end_date - timedelta(days=365)

    start_2yr = start_current - timedelta(days=730)
    end_2yr = end_date - timedelta(days=730)

    # Filter data for region
    region_data = df[df['regionid'] == region].copy()

    # Extract data for each period
    current_data = region_data[
        (region_data['settlementdate'] >= start_current) &
        (region_data['settlementdate'] <= end_date)
    ].copy()

    yr1_data = region_data[
        (region_data['settlementdate'] >= start_1yr) &
        (region_data['settlementdate'] <= end_1yr)
    ].copy()

    yr2_data = region_data[
        (region_data['settlementdate'] >= start_2yr) &
        (region_data['settlementdate'] <= end_2yr)
    ].copy()

    # Calculate hour of day for each period
    current_data['hour'] = current_data['settlementdate'].dt.hour
    yr1_data['hour'] = yr1_data['settlementdate'].dt.hour
    yr2_data['hour'] = yr2_data['settlementdate'].dt.hour

    # Calculate average by hour of day
    current_avg = current_data.groupby('hour')['power'].mean()
    yr1_avg = yr1_data.groupby('hour')['power'].mean()
    yr2_avg = yr2_data.groupby('hour')['power'].mean()

    # Create DataFrame for plotting
    plot_df = pd.DataFrame({
        'Hour': range(24),
        f'Current ({start_current.strftime("%b %d")} - {end_date.strftime("%b %d %Y")})': [current_avg.get(h, 0) for h in range(24)],
        f'1 Year Ago ({start_1yr.strftime("%b %d")} - {end_1yr.strftime("%b %d %Y")})': [yr1_avg.get(h, 0) for h in range(24)],
        f'2 Years Ago ({start_2yr.strftime("%b %d")} - {end_2yr.strftime("%b %d %Y")})': [yr2_avg.get(h, 0) for h in range(24)],
    })

    # Create plot
    plot = plot_df.hvplot.line(
        x='Hour',
        y=[col for col in plot_df.columns if col != 'Hour'],
        title=f'{region} - Rooftop Solar by Time of Day (30-day Average)',
        xlabel='Hour of Day',
        ylabel='Average Rooftop Solar (MW)',
        width=900,
        height=500,
        line_width=2.5,
        grid=True,
        legend='top_left'
    ).opts(
        bgcolor='#282a36',
        show_grid=True,
        gridstyle={'grid_line_alpha': 0.3}
    )

    # Add info panel
    info_text = f"""
    **Data Summary for {region}:**

    - **Current Period:** {len(current_data):,} records
    - **1 Year Ago:** {len(yr1_data):,} records
    - **2 Years Ago:** {len(yr2_data):,} records

    **Peak Output (MW):**
    - Current: {current_avg.max():.1f} MW at {current_avg.idxmax()}:00
    - 1 Year Ago: {yr1_avg.max():.1f} MW at {yr1_avg.idxmax()}:00
    - 2 Years Ago: {yr2_avg.max():.1f} MW at {yr2_avg.idxmax()}:00
    """

    return pn.Column(plot, pn.pane.Markdown(info_text))

# Create dashboard
dashboard = pn.Column(
    pn.pane.Markdown("""
    # Rooftop Solar Time-of-Day Comparison

    This dashboard compares rooftop solar output patterns by hour of day across three periods:
    - **Current:** Last 30 days
    - **1 Year Ago:** Same 30-day period last year
    - **2 Years Ago:** Same 30-day period 2 years ago

    **Data Source:** rooftop30.parquet (30-minute rooftop solar actuals)
    """),
    region_selector,
    create_time_of_day_plot
)

# Serve the dashboard
if __name__ == '__main__':
    print("\n" + "="*70)
    print("Starting Rooftop Solar Time-of-Day Dashboard")
    print("="*70)
    print("Open browser to: http://localhost:5021")
    print("Press Ctrl+C to stop")
    print("="*70 + "\n")

    dashboard.show(port=5021)
