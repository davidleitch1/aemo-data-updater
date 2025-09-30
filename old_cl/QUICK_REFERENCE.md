# AEMO Data Collection - Quick Reference

## Daily Operations

### Check Data Status
```bash
# See what months we have
ls -la mmsdm_data/*.parquet | wc -l

# Check latest data
python -c "import pandas as pd; df=pd.read_parquet('mmsdm_data/dispatch_unit_scada_30min_202507.parquet'); print(f'Latest: {df.SETTLEMENTDATE.max()}')"
```

### Update Current Data
```bash
# Get latest generator data
python dispatch_scada_processor.py

# Get latest prices/interconnectors  
python tradingis_processor.py
```

### Process Missing Historical Data
```bash
# Check what's missing
python check_missing_months.py

# Process specific month
python mmsdm_processor_optimized.py 2024 8

# Process all missing
python process_all_missing_scada.py
```

## Data Locations

### Current Data (Last 2 days)
- Generator output: https://nemweb.com.au/Reports/Current/Dispatch_SCADA/
- Prices/Interconnectors: https://nemweb.com.au/Reports/Current/TradingIS_Reports/

### Archive Data
- MMSDM (to July 2024): https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/
- DISPATCH_SCADA (Aug 2024+): https://nemweb.com.au/Reports/Archive/Dispatch_SCADA/
- TradingIS (Recent): https://nemweb.com.au/Reports/Archive/TradingIS_Reports/

## File Formats

### Download Examples
```python
# MMSDM monthly archive
url = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2024/MMSDM_2024_07.zip"

# DISPATCH_SCADA daily archive  
url = "https://nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20240801.zip"

# TradingIS weekly archive
url = "https://nemweb.com.au/Reports/Archive/TradingIS_Reports/PUBLIC_TRADINGIS_20240801_20240807.zip"
```

### CSV Structure
```
C,NEMP.WORLD,DISPATCH,...                    # Comment
I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,...   # Header  
D,DISPATCH,UNIT_SCADA,1,"2024/07/16 08:45:00",BARCSF1,14.50,... # Data
```

## Common Tasks

### Find DUIDs for a Region
```python
import pandas as pd
df = pd.read_parquet('mmsdm_data/unit_scada_30min_202407.parquet')
# Load region mapping from a price file to get REGIONID
price_df = pd.read_parquet('mmsdm_data/trading_price_202407.parquet')
# Analysis code here...
```

### Calculate Daily Generation
```python
df = pd.read_parquet('mmsdm_data/dispatch_unit_scada_30min_202407.parquet')
daily = df.groupby([pd.Grouper(key='SETTLEMENTDATE', freq='D'), 'DUID'])['MW'].sum() / 2  # MW to MWh
```

### Check Interconnector Flows
```python
df = pd.read_parquet('mmsdm_data/interconnector_flows_202407.parquet')
# Positive = North/East, Negative = South/West
nsw_to_qld = df[df['INTERCONNECTORID'] == 'N-Q-MNSP1']
```

## Troubleshooting

### No Data Extracted
- Check the date - DISPATCH_SCADA starts July 2024
- Verify the CSV parser indices match the file format

### Timeout Errors  
- Use conda python: `/Users/davidleitch/miniforge3/bin/python3`
- Process smaller date ranges
- Check internet connection

### Missing Dependencies
```bash
conda install pandas pyarrow requests beautifulsoup4
```