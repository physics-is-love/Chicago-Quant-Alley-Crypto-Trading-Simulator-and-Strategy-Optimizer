import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

headers = {
    'Accept': 'application/json'
}

url = "https://api.delta.exchange/v2/history/candles"

# Define the perpetual futures symbol
PERPETUAL_FUTURES_SYMBOL = "BTCUSDT"

# Loop through each date from May 19 to May 25
for i in range(1, 8):
    start_time = datetime(2025, 5, 18 + i, 9, 0, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=1)
    start_unix = int(start_time.timestamp())
    end_unix = int(end_time.timestamp())
    
    date_str = start_time.strftime("%Y-%m-%d")
    
    # Create the directory for the current date if it doesn't exist
    daily_data_dir = os.path.join("data", start_time.strftime('%Y%m%d'))
    os.makedirs(daily_data_dir, exist_ok=True)

    print(f"\n--- Fetching data for {date_str} ---")

    # --- Fetch Perpetual Futures Data ---
    futures_df = pd.DataFrame()
    
    print(f"Trying to fetch data for perpetual futures: {PERPETUAL_FUTURES_SYMBOL}")

    params = {
        "resolution": "5m", # Adjust resolution as needed (e.g., "1m", "1h")
        "symbol": PERPETUAL_FUTURES_SYMBOL,
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
            df["symbol"] = PERPETUAL_FUTURES_SYMBOL # Explicitly set symbol
            
            # Save the futures data to a CSV in the daily folder
            futures_file_path = os.path.join(daily_data_dir, f"{PERPETUAL_FUTURES_SYMBOL}.csv")
            df.to_csv(futures_file_path, index=False)
            print(f"Saved perpetual futures data to {futures_file_path}")
        else:
            print(f"No candlestick data returned for {PERPETUAL_FUTURES_SYMBOL}.")
    else:
        print(f"API request failed for {PERPETUAL_FUTURES_SYMBOL} with status code {response.status_code}: {response.text}")

    # --- Your existing logic to fetch and save Call/Put Options Data ---
    # It's good practice to integrate this into the same daily loop if possible
    # to ensure all data for a specific day is in its correct folder.
    # I'm slightly modifying your existing options saving part to use the daily_data_dir.

    calls_df = pd.DataFrame()
    puts_df = pd.DataFrame()

    # Assuming expiry_date_obj is calculated correctly for options for each day
    # For the assignment, it might be simpler to use a fixed expiry or calculate it dynamically
    # based on the options available for that specific date in the data.
    # For now, I'll keep your original expiry_date calculation for consistency with your previous code.
    expiry_date_obj = start_time + timedelta(days=3) # Your original calculation
    expiry_date = expiry_date_obj.strftime("%d%m%y") # Your original calculation

    for strike_price in range(90000, 117000, 200):
        for option_type_char in ['C', 'P']: # Renamed to avoid conflict with 'option_type' column name
            symbol = f"{option_type_char}-BTC-{strike_price}-{expiry_date}"
            # print(f"Trying to fetch data for: {symbol}") # Keep this for debugging if needed

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
                    df["option_type"] = "call" if option_type_char == 'C' else "put"

                    if option_type_char == 'C':
                        calls_df = pd.concat([calls_df, df], ignore_index=True)
                    else:
                        puts_df = pd.concat([puts_df, df], ignore_index=True)
                # else:
                #     print(f"No candlestick data returned for {symbol}.") # Uncomment for more verbose output
            # else:
            #     print(f"API request failed for {symbol} with status code {response.status_code}: {response.text}") # Uncomment for more verbose output

    # Save daily options files into the respective daily subfolder
    if not calls_df.empty:
        calls_file_path = os.path.join(daily_data_dir, f"calls_{date_str}.csv")
        calls_df.to_csv(calls_file_path, index=False)
        print(f"Saved call options data to {calls_file_path}")

    if not puts_df.empty:
        puts_file_path = os.path.join(daily_data_dir, f"puts_{date_str}.csv")
        puts_df.to_csv(puts_file_path, index=False)
        print(f"Saved put options data to {puts_file_path}")

print("\nData fetching complete!")