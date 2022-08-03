import requests
import sqlite3
import time
import math
from tqdm import tqdm

# connect to database and create table
con = sqlite3.connect('price_data.db')
cur = con.cursor()

# config
coin_pairs = ["XNO-USDT", "SCRT-USDT", "ANC-USDT", "YFI-USDT", "SKL-USDT", "SLP-USDT", "AERGO-USDT", "PRQ-USDT", "VELO-USDT", "DERO-USDT", "KMD-USDT", "LINA-USDT", "OVR-USDT", "NYM-USDT", "ASTR-USDT", "ANT-USDT", "QI-USDT", "BSW-USDT", "MTRG-USDT", "MTL-USDT"]
timeframe = "1hour" # 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week
timeframe_in_seconds = 1 * 60 * 60 # change to match timeframe
current_timestamp = math.floor(time.time())
start_at = current_timestamp - 365 * 24 * 60 * 60
end_at = current_timestamp
pieces_of_data = 1500 # kucoin API returns at most 1500 pieces of data for each query

# create table
cur.execute(f'CREATE TABLE IF NOT EXISTS "{timeframe}" (pair text, time int, open real, close real, high real, low real, volume real, turnover real)')

for pair in coin_pairs:
  print(f'{pair} start')
  # generate start and end times
  next_start_at = start_at
  last_insert_time = cur.execute(f'SELECT MAX(time) FROM "{timeframe}" WHERE pair=?', [pair]).fetchone()[0]
  if last_insert_time is not None:
    next_start_at = last_insert_time + timeframe_in_seconds
  times = []
  while next_start_at < end_at:
    current_start_at = next_start_at
    current_end_at = current_start_at + timeframe_in_seconds * pieces_of_data
    if current_end_at > end_at:
      current_end_at = end_at
    times.append((current_start_at, current_end_at))
    next_start_at = current_end_at

  # get and store data
  for current_batch in tqdm(times):
    (current_start_at, current_end_at) = current_batch
    res = requests.get(f'https://api.kucoin.com/api/v1/market/candles?type={timeframe}&symbol={pair}&startAt={current_start_at}&endAt={current_end_at}')
    res = res.json()
    if res["code"] != "200000": # ok
      tqdm.write(f'error code: {res["code"]}, will try this batch again at the end: {current_batch}')
      if res["code"] == "429000": # request rate limit is exceeded
        tqdm.write("If this is not stuck in a loop then the data should be fine")
      times.append(current_batch)
      continue
    data = res["data"]
    cur.executemany(f'INSERT INTO "{timeframe}" VALUES ("{pair}", ?, ?, ?, ?, ?, ?, ?)', data)
    con.commit()
    #time.sleep() # if stuck in error 429000 loop then maybe sleeping here would help, but in my case it didn't have much effect 

  print(f'{pair} done')

con.close()



# candle = cur.execute('SELECT * FROM pair').fetchone()
# (time, open, close, high, low, volume, turnover) = candle