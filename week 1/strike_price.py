import requests
import csv
headers = {
  'Accept': 'application/json'
}

r = requests.get('https://api.india.delta.exchange/v2/products/?states=expired', params={'contract_types': 'call_options,put_options', 'underlying_asset_symbols':'BTC', 'expiry_date':'01-01-2025'}, headers = headers)

print (r.json())

data = r.json()

# 3. Verify success and extract the list
if not data.get("success", False):
    raise RuntimeError(f"API returned success=False: {data}")

tickers = data.get("result", [])
if not isinstance(tickers, list):
    raise RuntimeError(f"Unexpected format for 'result': {tickers!r}")

# 4. Open a CSV file and write only the "symbol" column
csv_filename = "symbols.csv"
with open(csv_filename, mode="w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    # Write a header row
    writer.writerow(["symbol"])
    
    # Write each ticker’s symbol
    for item in tickers:
        symbol = item.get("symbol")
        if symbol is not None:
            writer.writerow([symbol])

print(f"✅ Saved {len(tickers)} symbols to {csv_filename}")