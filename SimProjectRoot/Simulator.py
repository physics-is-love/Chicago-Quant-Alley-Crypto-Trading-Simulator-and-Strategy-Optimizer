# Simulator.py

import os
import pandas as pd
from datetime import timedelta
from config import simStartDate, simEndDate, symbols, data_path
from Strategy import Strategy
from stats.printStats import printStats  # Moved here
from glob import glob
import csv 

class Simulator:
    def __init__(self):
        self.df = None
        self.currentPrice = {}
        self.currQuantity = {}
        self.buyValue = {}
        self.sellValue = {}
        self.pnl_history = []
        self.strategy = Strategy(self)
        self.pnl_records = [] 

    def readData(self):
        all_data = []
        date = simStartDate
        while date <= simEndDate:
            folder = os.path.join(data_path, date.strftime('%Y%m%d'))
            if not os.path.exists(folder):
                print(f"Warning: folder not found for date {date.strftime('%Y-%m-%d')}")
                date += timedelta(days=1)
                continue

            for file_path in glob(os.path.join(folder, '*.csv')):
                df = pd.read_csv(file_path)
                
                # Check if it's an options file by looking for 'option_type' and 'strike_price'
                if 'option_type' in df.columns and 'strike_price' in df.columns:
                    # It's an options file, keep all relevant columns
                    if {'symbol', 'time', 'close', 'strike_price', 'option_type'}.issubset(df.columns):
                        df_processed = df[['time', 'symbol', 'close', 'strike_price', 'option_type']].copy()
                    else:
                        print(f"Skipped options file (missing required columns): {file_path}")
                        continue # Skip to the next file
                else:
                    # Assume it's a futures/spot file (like BTCUSDT.csv)
                    if {'symbol', 'time', 'close'}.issubset(df.columns):
                        df_processed = df[['time', 'symbol', 'close']].copy()
                    else:
                        print(f"Skipped futures/spot file (missing required columns): {file_path}")
                        continue # Skip to the next file

                df_processed.rename(columns={'close': 'price'}, inplace=True)
                all_data.append(df_processed)
            
            date += timedelta(days=1)
                                                       
        if all_data:
            self.df = pd.concat(all_data, ignore_index=True) # Add ignore_index=True for cleaner concat
            self.df['time'] = pd.to_datetime(self.df['time'])
            self.df.sort_values('time', inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            # Fill NaN values for 'option_type' and 'strike_price' that will exist for futures data
            # This is important because futures data won't have these columns, and concat will fill them with NaN
            self.df['option_type'] = self.df['option_type'].fillna('') # Fill with empty string or another suitable default
            self.df['strike_price'] = self.df['strike_price'].fillna(0) # Fill with 0 or another suitable default
        else:
            raise ValueError("No valid data files found.")


    def startSimulation(self):
        last_processed_date = None
        for _, row in self.df.iterrows():
            symbol = row['symbol']
            price = row['price']
            self.currentPrice[symbol] = price
            self.strategy.onMarketData(row)

            current_date = row['time'].date()
            if last_processed_date is None:
                last_processed_date = current_date

            if current_date != last_processed_date:
                self.printPnl(timestamp=last_processed_date) # Record P&L at end of day
                last_processed_date = current_date

        self.printPnl(timestamp=self.df['time'].iloc[-1].date())

    def onOrder(self, symbol, side, quantity, price):
        epsilon = 0.0001
        trade_price = price * (1 + epsilon) if side == 'BUY' else price * (1 - epsilon)
        trade_value = trade_price * quantity

        if side == 'BUY':
            self.currQuantity[symbol] = self.currQuantity.get(symbol, 0) + quantity
            self.buyValue[symbol] = self.buyValue.get(symbol, 0) + trade_value
        else:
            self.currQuantity[symbol] = self.currQuantity.get(symbol, 0) - quantity
            self.sellValue[symbol] = self.sellValue.get(symbol, 0) + trade_value

        self.strategy.onTradeConfirmation(symbol, side, quantity, trade_price)

    def printPnl(self, timestamp=None):
        total_pnl = 0
        for symbol in set(self.buyValue) | set(self.sellValue):
            buy_val = self.buyValue.get(symbol, 0)
            sell_val = self.sellValue.get(symbol, 0)
            quantity = self.currQuantity.get(symbol, 0)
            current_price = self.currentPrice.get(symbol, 0)
            pnl = sell_val - buy_val + quantity * current_price
            total_pnl += pnl

        self.pnl_history.append(total_pnl)
        ts = timestamp if timestamp else "Final"
        self.pnl_records.append({'time': ts, 'PnL': total_pnl})
        print(f"Current Total P&L at {ts}: {total_pnl:.2f}")
    
    def exportPnlToCsv(self, output_file='output.csv'):
        df_pnl = pd.DataFrame(self.pnl_records)
        df_pnl.to_csv(output_file, index=False)
        print(f"P&L history exported to {output_file}")



if __name__ == '__main__':
    sim = Simulator()
    sim.readData()
    sim.startSimulation()
    sim.printPnl()
    printStats(sim.pnl_history)
    sim.exportPnlToCsv()
