import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

headers = {
    'Accept': 'application/json'
}

url = "https://api.delta.exchange/v2/history/candles"

# Loop through each date from May 19 to May 25
for i in range(1, 8):
    start_time = datetime(2025, 5, 18 + i, 9, 0, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=1)
    start_unix = int(start_time.timestamp())
    end_unix = int(end_time.timestamp())
    
    expiry_date_obj = start_time + timedelta(days=3)
    expiry_date = expiry_date_obj.strftime("%d%m%y")
    
    date_str = start_time.strftime("%Y-%m-%d")  

    calls_df = pd.DataFrame()
    puts_df = pd.DataFrame()

    for strike_price in range(90000, 117000, 200):
        for option_type in ['C', 'P']:  
            symbol = f"{option_type}-BTC-{strike_price}-{expiry_date}"
            print(f"Trying to fetch data for: {symbol}")

            params = {
                "resolution": "5m",
                "symbol": symbol,
                "start": start_unix,
                "end": end_unix
            }

            response = requests.get(url, params=params)

            if response.status_code == 200:
                result = response.json().get("result", [])
                if result:
                    df = pd.DataFrame(result)
                    df["time"] = pd.to_datetime(df["time"], unit="s")
                    df = df.sort_values("time")
                    df["symbol"] = symbol
                    df["strike_price"] = strike_price
                    df["option_type"] = "call" if option_type == 'C' else "put"

                    if option_type == 'C':
                        calls_df = pd.concat([calls_df, df])
                    else:
                        puts_df = pd.concat([puts_df, df])
                else:
                    print(f"No candlestick data returned for {symbol}.")
            else:
                print(f"API request failed for {symbol} with status code {response.status_code}: {response.text}")

    # Save daily files
    if not calls_df.empty:
        calls_df.to_csv(f"calls_{date_str}.csv", index=False)
        print(f"Saved call options data to calls_{date_str}.csv")

    if not puts_df.empty:
        puts_df.to_csv(f"puts_{date_str}.csv", index=False)
        print(f"Saved put options data to puts_{date_str}.csv")