import math
from pycoingecko import CoinGeckoAPI
import pandas as pd
import sqlite3
import time

# connect to db
con = sqlite3.connect('data.db')
cur = con.cursor()

# connect to api
cg = CoinGeckoAPI()

# create table
cur.execute(f'CREATE TABLE IF NOT EXISTS "exchange_volumes" (time int, name text, trade_volume_24h_btc real)')

time = math.floor(time.time())

data_id = cg.get_exchanges_list()
df_id = pd.DataFrame(
    data_id, columns=['name', 'trade_volume_24h_btc'])
df_markets_all = df_id

cur.executemany(f'INSERT INTO exchange_volumes VALUES ("{time}", ?, ?)', df_markets_all.values.tolist())

con.commit()
con.close()