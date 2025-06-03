import pandas as pd
from binance.client import Client
import time
from datetime import datetime
import pytz
import util
import os

api_key = "XDMzJMp2pSYue50iDgFvXMqxGhxzlWeFlraBNBi53lyw4WKDO3I8TNkA0H2Uw51R"
api_secret = "dlFdc3ZnSUBQOt6L1dWCBurhI5TOK88oT1JeUmeFW11jIolpifSoMC5Resm9NU8T"
client = Client(api_key, api_secret)

symbols = ['BTCUSDT']
interval = Client.KLINE_INTERVAL_1HOUR
limit = 1000
start_time_str = "2021-01-01 00:00:00"
end_time_str = "2025-01-01 00:00:00"

start_time = util.str_to_timestamp(start_time_str)
end_time = util.str_to_timestamp(end_time_str)

# Collect data for all symbols into a dictionary of DataFrames
dataframes = {}
for symbol in symbols:
    klines = util.get_kline_time(client, symbol, interval, start_time, end_time, limit)
    symbol_data = [{
        'datetime': util.timestamp_to_datetime(kline[0]),
        'close': float(kline[4])  # Closing price
    } for kline in klines]
    dataframes[symbol] = pd.DataFrame(symbol_data)

# Merge DataFrames on 'datetime'
df = dataframes[symbols[0]]
for symbol in symbols[1:]:
    df = df.merge(dataframes[symbol], on='datetime', how='outer')

# Sort by datetime to ensure chronological order
df = df.sort_values('datetime').reset_index(drop=True)

print(df)
output_dir = r'/Users/chunhen/Documents/Quant Project/candle'
os.makedirs(output_dir, exist_ok=True)

csv_filename = f'{symbol}_{interval}_candle_binance.csv'
df.to_csv(os.path.join(output_dir, csv_filename), index=False)