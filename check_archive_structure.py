#!/usr/bin/env python3
"""
Check AEMO archive structure for DISPATCHSCADA files
"""

import requests
from bs4 import BeautifulSoup

# Check the ARCHIVE structure for Dispatch_SCADA
print('Checking ARCHIVE structure for Dispatch_SCADA...')

# Try different URL patterns
urls_to_test = [
    'http://nemweb.com.au/Reports/Archive/Dispatch_SCADA/',
    'http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/',
    'http://www.nemweb.com.au/Reports/Archive/Dispatch_SCADA/',
    'http://nemweb.com.au/Reports/Current/Dispatch_SCADA/'
]

for url in urls_to_test:
    print(f'\nTrying: {url}')
    try:
        response = requests.get(url, timeout=10)
        print(f'Status: {response.status_code}')
        if response.status_code == 200:
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            # Look for monthly folders
            folders = [l['href'] for l in links if l['href'].endswith('/') and l['href'] != '../']
            print(f'Found {len(folders)} folders')
            if folders:
                # Show recent folders
                recent = sorted([f for f in folders if f.startswith('202')])[-10:]
                print(f'Recent folders: {recent}')
                
                # Check if July 2025 exists
                if '202507/' in folders:
                    print('\n✅ July 2025 folder found!')
                    july_url = url + '202507/'
                    july_resp = requests.get(july_url, timeout=10)
                    if july_resp.status_code == 200:
                        july_soup = BeautifulSoup(july_resp.content, 'html.parser')
                        july_links = july_soup.find_all('a', href=True)
                        july_files = [l['href'] for l in july_links if '.zip' in l['href'].lower()]
                        print(f'July 2025 contains {len(july_files)} files')
                        if july_files:
                            # Show sample files
                            sample = [f for f in july_files if '20250717' in f][:3]
                            print(f'July 17 files: {sample}')
                
            # Look for direct files
            files = [l['href'] for l in links if '.zip' in l['href'].lower()]
            if files:
                print(f'Found {len(files)} zip files directly')
                # Show sample
                sample = [f for f in files if 'DISPATCHSCADA' in f and '202507' in f][:3]
                if sample:
                    print(f'July 2025 sample files: {sample}')
    except Exception as e:
        print(f'Error: {e}')

print('\n' + '='*60)
print('Summary: Archive files appear to be in the CURRENT folder for recent dates')
print('For July 17-Aug 20, 2025, we should check the CURRENT folder')