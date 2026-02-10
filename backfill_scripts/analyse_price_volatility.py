#!/usr/bin/env python3
"""
Quick analysis of price volatility over time using rolling standard deviation

Usage:
    python analyse_price_volatility.py

Then open browser to http://localhost:5007
"""

import pandas as pd
import panel as pn
import hvplot.pandas

# Enable Panel extension
pn.extension()

# Load prices5 data
print("Loading prices5.parquet...")
df = pd.read_parquet('/Volumes/davidleitch/aemo_production/data/prices5.parquet')
print(f"Loaded {len(df):,} records from {df['settlementdate'].min()} to {df['settlementdate'].max()}")

# Prepare data - pivot to have regions as columns
df_pivot = df.pivot(index='settlementdate', columns='regionid', values='rrp')
print(f"Pivoted data: {df_pivot.shape[0]:,} rows x {df_pivot.shape[1]} regions")

# Create widgets
region_selector = pn.widgets.Select(
    name='Region',
    options=['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
    value='NSW1'
)

rolling_period_selector = pn.widgets.Select(
    name='Rolling Period',
    options={'30 days': 30, '90 days': 90},
    value=30
)

@pn.depends(region_selector.param.value, rolling_period_selector.param.value)
def create_volatility_plot(region, rolling_days):
    """Create rolling standard deviation plot for selected region and period"""

    # Calculate rolling std (convert days to 5-min intervals: 288 per day)
    rolling_intervals = rolling_days * 288

    # Get region data
    region_data = df_pivot[region].copy()

    # Calculate rolling std
    rolling_std = region_data.rolling(window=rolling_intervals, min_periods=1).std()

    # Create DataFrame for plotting
    plot_df = pd.DataFrame({
        'Date': rolling_std.index,
        'Rolling Std Dev ($/MWh)': rolling_std.values
    }).set_index('Date')

    # Create plot
    plot = plot_df.hvplot.line(
        title=f'{region} - Price Volatility ({rolling_days}-day Rolling Std Dev)',
        xlabel='Date',
        ylabel='Rolling Standard Deviation ($/MWh)',
        width=900,
        height=500,
        color='#ff5555',
        line_width=2,
        grid=True
    ).opts(
        bgcolor='#282a36',
        show_grid=True,
        gridstyle={'grid_line_alpha': 0.3}
    )

    return plot

# Create dashboard
dashboard = pn.Column(
    pn.pane.Markdown("""
    # Price Volatility Analysis - Rolling Standard Deviation

    This dashboard shows how price volatility has changed over time using rolling standard deviation.
    Higher values indicate more volatile prices (greater price swings).

    **Data:** prices5.parquet (5-minute spot prices)
    **Date Range:** 2022-01-01 to 2025-10-17
    """),
    pn.Row(region_selector, rolling_period_selector),
    create_volatility_plot
)

# Serve the dashboard
if __name__ == '__main__':
    print("\n" + "="*70)
    print("Starting Price Volatility Dashboard")
    print("="*70)
    print("Open browser to: http://localhost:5020")
    print("Press Ctrl+C to stop")
    print("="*70 + "\n")

    dashboard.show(port=5020)
