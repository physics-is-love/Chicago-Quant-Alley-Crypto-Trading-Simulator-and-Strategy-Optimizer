# import pandas as pd
# from utils.getStrikes import get_closest_strikes

# class Strategy:
#     def __init__(self, simulator):
#         self.sim = simulator
#         self.entry_price = None
#         self.entry_time = None
#         self.call_symbol = None
#         self.put_symbol = None
#         self.position_open = False
#         self.trades = []
#         self.total_pnl = 0

#     def onMarketData(self, row):
#         time = row['time']
#         symbol = row['symbol']
#         price = row['price']

#         if time.hour == 13 and time.minute == 0 and not self.position_open:
#             if symbol == 'BTCUSDT': # Exact match for futures symbol
#                 self.entry_time = time
#                 self.entry_price = price
#         # Pass all market data for the current timestamp to get_closest_strikes
#         # Or, even better, have the simulator pass relevant daily options data to strategy once a day
#         # For now, let's pass a reference to the simulator's full DF and filter inside get_closest_strikes
#         # This is not ideal for performance but gets it working

#         # Get all rows for the current timestamp (which might include options)
#             current_timestamp_data = self.sim.df[self.sim.df['time'] == time]
#             daily_options_df = current_timestamp_data[current_timestamp_data['option_type'].notna()] # Filter for options


#             self.call_symbol, self.put_symbol = get_closest_strikes(price, daily_options_df)

#             if self.call_symbol and self.put_symbol: # Only place orders if symbols were found
#             # Ensure you use the current market price of the specific option symbol
#                 call_current_price = self.sim.currentPrice.get(self.call_symbol, price) # Default to futures price if not found (fallback)
#                 put_current_price = self.sim.currentPrice.get(self.put_symbol, price) # Default to futures price if not found (fallback)

#                 self.sim.onOrder(self.call_symbol, 'SELL', 0.1, call_current_price)
#                 self.sim.onOrder(self.put_symbol, 'SELL', 0.1, put_current_price)
#                 self.position_open = True
#             else:
#                 print(f"Could not find ATM call/put for {time} at futures price {price}")


#         if self.position_open:
#     # Ensure you use the latest known futures price for deviation check
#     # Assuming 'BTCUSDT' is the symbol for futures
#             futures_current_price = self.sim.currentPrice.get('BTCUSDT', self.entry_price) # Get latest futures price

#     # IMPORTANT: Use futures_current_price for deviation calculation
#             deviation = abs(futures_current_price - self.entry_price) / self.entry_price

#             if deviation > 0.01 or abs(self.total_pnl) > 500:
#         # When exiting, ensure you are buying back the *correct* options at their *current* prices
#                 call_buy_price = self.sim.currentPrice.get(self.call_symbol, futures_current_price)
#                 put_buy_price = self.sim.currentPrice.get(self.put_symbol, futures_current_price)

#                 self.sim.onOrder(self.call_symbol, 'BUY', 0.1, call_buy_price)
#                 self.sim.onOrder(self.put_symbol, 'BUY', 0.1, put_buy_price)
#                 self.position_open = False

#     def onTradeConfirmation(self, symbol, side, quantity, price):
#         direction = 1 if side == 'SELL' else -1
#         self.total_pnl += direction * quantity * price
#         self.trades.append({'symbol': symbol, 'side': side, 'qty': quantity, 'price': price})

import pandas as pd
from utils.getStrikes import get_closest_strikes # Ensure this import is correct

class Strategy:
    def __init__(self, simulator):
        self.sim = simulator
        self.entry_price = None
        self.entry_time = None
        self.call_symbol = None
        self.put_symbol = None
        self.position_open = False
        self.trades = []
        self.total_pnl = 0 # This will track a basic PnL for strategy's internal use

    def onMarketData(self, row):
        time = row['time']
        symbol = row['symbol']
        price = row['price']

        # Always update the simulator's current price for the symbol received
        # This ensures currentPrice dict has the latest values for all instruments
        self.sim.currentPrice[symbol] = price 

        # --- Entry Logic ---
        # Trigger entry only if it's 1 PM, no position is open, AND the current row is the BTCUSDT futures
        if time.hour == 13 and time.minute == 0 and not self.position_open:
            if symbol == 'BTCUSDT': # Ensure this row is for the futures contract
                self.entry_time = time
                self.entry_price = price # Store the futures price at entry
                
                # Call get_closest_strikes with the futures price, current time, and the full simulation DataFrame
                self.call_symbol, self.put_symbol = get_closest_strikes(
                    futures_price=self.entry_price,     # The futures price at 1 PM
                    current_time_obj=time,              # The current timestamp
                    all_sim_df=self.sim.df              # The entire DataFrame of loaded data
                )
                
                if self.call_symbol and self.put_symbol: # Only proceed if both ATM call and put symbols were found
                    # Get the actual current market price of the selected options from simulator's currentPrice
                    # Use the futures price as a fallback if the option price isn't yet updated in currentPrice
                    call_current_price = self.sim.currentPrice.get(self.call_symbol, price)
                    put_current_price = self.sim.currentPrice.get(self.put_symbol, price)
                    
                    self.sim.onOrder(self.call_symbol, 'SELL', 0.1, call_current_price)
                    self.sim.onOrder(self.put_symbol, 'SELL', 0.1, put_current_price)
                    self.position_open = True
                    print(f"Opened position at {time}: Futures {self.entry_price:.2f}, Sold Call {self.call_symbol} at {call_current_price:.2f}, Sold Put {self.put_symbol} at {put_current_price:.2f}")
                else:
                    print(f"Could not find ATM call/put for {time} at futures price {self.entry_price:.2f}")

        # --- Exit Logic ---
        # Only check exit conditions if a position is open AND the current market data row is for BTCUSDT (futures)
        if self.position_open:
            if symbol == 'BTCUSDT': # We only use futures price for deviation check
                futures_current_price = price # This 'price' is already the BTCUSDT price for this row
                deviation = abs(futures_current_price - self.entry_price) / self.entry_price
                
                # Exit condition: futures price deviation OR strategy's internal P&L threshold
                # Note: self.total_pnl here is a simple sum of trade values, for a true P&L from straddle,
                # you'd track individual leg P&L or rely on simulator's comprehensive P&L.
                if deviation > 0.01 or abs(self.total_pnl) > 500:
                    # Get the actual current market price of the options to close the trade
                    call_buy_price = self.sim.currentPrice.get(self.call_symbol, futures_current_price)
                    put_buy_price = self.sim.currentPrice.get(self.put_symbol, futures_current_price)
                    
                    self.sim.onOrder(self.call_symbol, 'BUY', 0.1, call_buy_price)
                    self.sim.onOrder(self.put_symbol, 'BUY', 0.1, put_buy_price)
                    self.position_open = False
                    print(f"Closed position at {time}: Futures {futures_current_price:.2f}, Strategy P&L {self.total_pnl:.2f}, Deviation {deviation:.4f}")

    def onTradeConfirmation(self, symbol, side, quantity, price):
        # This method records individual trades and updates a simplified strategy-level P&L.
        # For a more precise realized P&L of the straddle, you would need to track
        # the entry price for each leg (call/put) and calculate (exit_price - entry_price) * quantity.
        # The simulator's printPnl will give the comprehensive P&L.
        
        # This simple accumulation works for tracking if the sum of all transaction values crosses a threshold.
        # When selling, you receive money (positive for this total_pnl).
        # When buying, you pay money (negative for this total_pnl).
        # The absolute value `abs(self.total_pnl)` in exit condition then makes sense for total P&L.
        direction = 1 if side == 'SELL' else -1 # Sell increases PnL, Buy decreases PnL (from cash perspective)
        self.total_pnl += direction * quantity * price 
        
        self.trades.append({'symbol': symbol, 'side': side, 'qty': quantity, 'price': price})
        print(f"Trade confirmed: {side} {quantity} {symbol} at {price:.2f}. Current Strategy Total P&L: {self.total_pnl:.2f}")
