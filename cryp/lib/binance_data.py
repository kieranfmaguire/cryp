"""
Class to request and parse market data from Binance exchance, using ccxt package
"""

import ccxt
import pandas as pd
import datetime
import pytz

class BinanceDataCollector(ccxt.binance):

    def __init__(self, enable_rate_limit=True):
        """
        Subclass the ccxt.binance API to collect and parse some data
        :param enable_rate_limit: Should the rate limiter be enabled. Default is true. If not, risk getting banned.
        :return: None
        """

        super().__init__()

        self.TIMEZONE = pytz.UTC
        self.DEFAULT_SYMBOL = 'LTC/USDT'
        self.BUFFER_SIZE_ROWS = 10000

        self.enableRateLimit = enable_rate_limit
        self.order_book = None

    def get_order_book_with_time(self, symbol=None, **kwargs):
        """
        Get orderbook snapshot for list of tickers from the REST API. Add timestamps (in UTC)
        :param symbols:
        :return: dict
        """
        if symbol is None:
            symbol = self.DEFAULT_SYMBOL
        request_time = datetime.datetime.now(self.TIMEZONE)
        order_book = self.fetch_l2_order_book(symbol=symbol, **kwargs)
        recv_time = datetime.datetime.now(self.TIMEZONE)
        order_book['request_time'] = request_time
        order_book['receive_time'] = recv_time
        return order_book

    def handle_order_book(self, order_book):
        """
        When a new order book snapshot is fetched, store in frame until enough rows have been collected,
        then push data out
        :param order_book:
        :return:
        """

        if self.order_book is None:
            self.reset_order_book()

        ask_prices, bid_prices = [x[0] for x in order_book['asks']], [x[1] for x in order_book['bids']]
        ask_quantities, bid_quantities = [x[0] for x in order_book['bids']], [x[1] for x in order_book['bids']]
        partial_df = pd.DataFrame(columns=self.order_book.reset_index().columns)

        partial_df['price'] = ask_prices + bid_prices
        partial_df['quantity'] = ask_quantities + bid_quantities
        partial_df['type'] = ['ask'] * len(ask_prices) + ['bid'] * len(bid_prices)
        partial_df['symbol'] = order_book['symbol']
        partial_df['received_dt'] = order_book['received_dt']
        partial_df['request_dt'] = order_book['request_dt']
        partial_df.set_index(self.order_book.index.names, inplace=True, append=False)

        self.order_book.append(partial_df)

        if self.order_book.shape[0] >= self.BUFFER_SIZE_ROWS:
            self.write_to_db()
            self.reset_order_book()

    def reset_order_book(self):
        """
        Set up data frame to store order book
        :return: None
        """

        self.order_book = pd.DataFrame(columns=['symbol', 'request_dt', 'received_dt', 'type', 'price', 'quantity'])\
            .set_index(['symbol', 'request_dt', 'received_dt', 'type'], append=False)

    def write_to_db(self):
        """
        Write out current order book to DB
        :return:
        """
        pass
