
# using ccxt package to connect to binance API and explore data available
# user manual here: https://github.com/ccxt/ccxt/wiki/Manual#overview

import ccxt
import pandas as pd
import numpy as np
import mysql.connector

# region testing connectiont to binance public API

# see what exchanges we can connect to
print(f"Available exchanges:")
for ex in ccxt.exchanges:
    print(f"\t{ex}")

# connect to binance (public)
binance = ccxt.binance()

# see what attributes the class has
print(f"Echange id / name: {binance.id} / {binance.name}")
print(f"Echange API has the following capabilities:")
for cap in binance.has:
    print(f"\t{cap}")

# load available markets
binance.load_markets()
print(f"Exchange has the following markets:")
for i, m in enumerate(binance.markets):
    print(f"\t{i} -> {m}")

print(f"Exchange has the following symbols:")
for i, s in enumerate(binance.symbols):
    print(f"\t{i} -> {s}")

print(f"Exchange has the following currencies:")
for i, c in enumerate(binance.currencies):
    print(f"\t{i} -> {c}")

# turn on the rate limiter to avoid getting banned for hitting the API too frequently
binance.enableRateLimit = True

# have a look at the market structure for BTC/USDT
btc_usdt = binance.markets['BTC/USDT']
print(f"Market structure for BTC/USDT:\n{btc_usdt}")

# see what info we can pull out for BTC/USDT market
# seems to print out aggregated prices over a certain time period (for example includes VWAP key)
# a little hard to decipher
binance.fetch_ticker('BTC/USDT')

# can also use the exchange specific API (not recommended) to get prices directly
binance.public_get_ticker_price({'symbol': 'BTCUSDT'})

# can get trades over recent time - there is a "since" argument which is specified in milliseconds. Builtin to get "now" milliseconds, then subtract
binance.fetch_trades('BTC/USDT', since=binance.milliseconds() - 2000)

# can look at snapshot of the order book
# for some reason the timestamp and datetime is coming back empty though ...
orderbook = binance.fetch_order_book('BTC/USDT')
print(orderbook)
print(orderbook.keys())

# here is a code snippet to fetch one days worth of trades pulled from the manual: https://github.com/ccxt/ccxt/wiki/Manual#overview
# it doesn't work because of the await call - need to figure out why
# Python
if binance.has['fetchOrders']:
    since = binance.milliseconds() - 86400000  # -1 day from now
    # alternatively, fetch from a certain starting datetime
    # since = exchange.parse8601('2018-01-01T00:00:00Z')
    all_orders = []
    while since < binance.milliseconds ():
        symbol = None  # change for your symbol
        limit = 20  # change for your limit
        orders = await binance.fetch_orders(symbol, since, limit)
        if len(orders):
            since = orders[len(orders) - 1]['timestamp']
            all_orders += orders
        else:
            break

# endregion

# region testing connection to mysql database

# following tutorial from: https://www.freecodecamp.org/news/connect-python-with-sql/

def create_db_connection(db_name, host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except BaseException as err:
        print(f"Error: '{err}'")

    return connection

def query(connection, query):
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute(query)
    except BaseException as err:
        print(f"Error: {err}")
    return cursor
connection = create_db_connection('crypto','localhost','researcher','researcher')

query(connection, 'SHOW TABLES;')

create_test_table = """
CREATE TABLE test_table (
id_key INT PRIMARY KEY,
age INT
);
"""

query(connection, create_test_table)
cursor = query(connection, "SHOW TABLES;") # the cursor is an iterable with the results I think
print(cursor)
# endregion




