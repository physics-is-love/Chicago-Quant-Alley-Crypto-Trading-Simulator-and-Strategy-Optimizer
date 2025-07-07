import pandas as pd
from datetime import timedelta

def get_closest_strikes(futures_price, current_time_obj, all_sim_df, price_deviation_percent=0.02):
    """
    Finds the closest At-The-Money (ATM) call and put option symbols for a given futures price.

    Args:
        futures_price (float): The current price of the underlying futures contract.
        current_time_obj (datetime.datetime): The current timestamp of the simulation.
        all_sim_df (pd.DataFrame): The entire DataFrame loaded by the simulator, containing
                                    both futures and options data with 'option_type' and 'strike_price' columns.
        price_deviation_percent (float): The maximum percentage deviation from the futures price
                                         for a strike to be considered ATM (e.g., 0.02 for 2%).

    Returns:
        tuple: (call_symbol, put_symbol) of the closest ATM options, or (None, None) if not found.
    """

    # Determine the target expiry date based on the current simulation day
    # This matches your data fetching script's logic: expiry is 3 days from the current day
    target_expiry_date_obj = current_time_obj.date() + timedelta(days=3)
    target_expiry_str_for_symbol = target_expiry_date_obj.strftime("%d%m%y") # Format: DDMMYY, e.g., '220525'

    # Filter the full simulation DataFrame for options relevant to the current day and target expiry
    # Ensure 'option_type' column is correctly filled (not NaN/empty string for options data)
    relevant_options = all_sim_df[
        (all_sim_df['time'].dt.date == current_time_obj.date()) &           # Options for the current simulation day
        (all_sim_df['option_type'].isin(['call', 'put'])) &                 # Ensure it's explicitly a 'call' or 'put'
        (all_sim_df['symbol'].str.contains(target_expiry_str_for_symbol, na=False)) # Match expiry in symbol string
    ].copy() # Use .copy() to avoid SettingWithCopyWarning if you modify this sub-DataFrame

    if relevant_options.empty:
        # print(f"DEBUG: No relevant options found for {current_time_obj.date()} with expiry {target_expiry_str_for_symbol}")
        return None, None

    # For each unique option symbol, get its latest price for the current day
    # This helps when multiple data points exist for the same option on the same day
    # Ensure 'time' column is datetime type before calling idxmax()
    if not pd.api.types.is_datetime64_any_dtype(relevant_options['time']):
        relevant_options['time'] = pd.to_datetime(relevant_options['time'])

    relevant_options_latest_prices = relevant_options.loc[relevant_options.groupby('symbol')['time'].idxmax()]

    atm_calls = []
    atm_puts = []

    for _, row in relevant_options_latest_prices.iterrows():
        strike = row['strike_price']
        option_type = row['option_type']
        symbol = row['symbol']

        # Ensure strike is not 0 (which it would be for futures data, but filtered above)
        # Check if strike is within the acceptable deviation from futures price
        if strike != 0 and abs(strike - futures_price) / futures_price <= price_deviation_percent:
            if option_type == 'call':
                atm_calls.append({'symbol': symbol, 'strike': strike})
            elif option_type == 'put':
                atm_puts.append({'symbol': symbol, 'strike': strike})
    
    call_symbol = None
    put_symbol = None

    if atm_calls:
        # Find the call with the strike closest to the futures_price
        closest_call = min(atm_calls, key=lambda x: abs(x['strike'] - futures_price))
        call_symbol = closest_call['symbol']
    if atm_puts:
        # Find the put with the strike closest to the futures_price
        closest_put = min(atm_puts, key=lambda x: abs(x['strike'] - futures_price))
        put_symbol = closest_put['symbol']

    return call_symbol, put_symbol