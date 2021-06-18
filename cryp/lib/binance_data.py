"""
Class to request and parse market data from Binance exchance, using ccxt package
"""

import ccxt
import pandas as pd
import time
import datetime
import pytz


class DataCollector(object):

    def __init__(self, exchange='binance', enable_rate_limit=True): #
        """
        Subclass the ccxt API to collect and parse some data
        :param exchange: str. Which exchange to connect to. Should be one of ccxt.exchanges
        :param enable_rate_limit: bool. Should the rate limiter be enabled. Default is true. If not, risk getting banned.
        :return: None
        """
        self.exchange = getattr(ccxt, exchange)()
        self.exchange.load_markets()
        self.TIMEZONE = pytz.UTC
        self.DEFAULT_SYMBOL = 'LTC/USDT'
        self.BUFFER_SIZE_ROWS = 10000

        self.exchange.enableRateLimit = enable_rate_limit
        self.order_book = None

    def watch_order_book(self, symbol=None, max_iter=None, **kwargs):
        """
        Request order book data at regular intervals (determined by exchange API rate limit), parse and yield
        :param symbol: str
        :param max_iter: int or None. If int, break loop after this many iters.
        :param kwargs: extra key=value args to be passed to the fetch order book call
        :return: generator. Each item is pd.DataFrame
        """

        if symbol is None:
            symbol = self.DEFAULT_SYMBOL
        if symbol not in self.exchange.symbols:
            print(f"Symbol is not available at this exchange!")
            return

        interval_milliseconds = self.exchange.rateLimit
        interval_seconds = interval_milliseconds * 1e-3

        i = 0
        while True:
            i += 1
            if i is None:
                pass
            else:
                if i > max_iter:
                    break
            time.sleep(interval_seconds)
            yield self.get_order_book_with_time(symbol, **kwargs)

    def get_order_book_with_time(self, symbol=None, **kwargs):
        """
        Get order book snapshot for list of tickers from the REST API. Add timestamps (in UTC), parse and return
        :param symbol: str
        :return: pd.DataFrame
        """
        if symbol is None:
            symbol = self.DEFAULT_SYMBOL
        if symbol not in self.exchange.symbols:
            print(f"Symbol is not available at this exchange!")
            return
        request_time = datetime.datetime.now(self.TIMEZONE)
        order_book = self.exchange.fetch_l2_order_book(symbol=symbol, **kwargs)
        recv_time = datetime.datetime.now(self.TIMEZONE)
        order_book['request_time'] = request_time
        order_book['receive_time'] = recv_time
        return self.parse_order_book(order_book)

    def parse_order_book(self, order_book):
        """
        When a new order book snapshot is fetched, shape into dataframe and add metadata

        :param order_book: Dict. the order book returned by `self.get_order_book_with_time()`
        :return: pd.DataFrame. formatted version of input order book
        """

        if self.order_book is None:
            self.reset_order_book()

        ask_prices, bid_prices = [x[0] for x in order_book['asks']], [x[0] for x in order_book['bids']]
        ask_quantities, bid_quantities = [x[1] for x in order_book['asks']], [x[1] for x in order_book['bids']]
        snapshot_order_book = pd.DataFrame(columns=self.order_book.reset_index().columns)

        snapshot_order_book['price'] = ask_prices + bid_prices
        snapshot_order_book['quantity'] = ask_quantities + bid_quantities
        snapshot_order_book['type'] = ['ask'] * len(ask_prices) + ['bid'] * len(bid_prices)
        snapshot_order_book['symbol'] = order_book['symbol']
        snapshot_order_book['received_dt'] = order_book['receive_time']
        snapshot_order_book['request_dt'] = order_book['request_time']
        snapshot_order_book['exchange'] = self.exchange
        snapshot_order_book.set_index(self.order_book.index.names, inplace=True, append=False)

        return snapshot_order_book

    def reset_order_book(self):
        """
        Set up data frame to store order book
        :return: None
        """

        self.order_book = pd.DataFrame(columns=['exchange', 'symbol', 'request_dt', 'received_dt', 'type', 'price', 'quantity'])\
            .set_index(['exchange', 'symbol', 'request_dt', 'received_dt', 'type'], append=False)

    def buffer_order_book(self):
        """

        :return:
        """

    def write_to_db(self):
        """
        Write out current order book to DB
        :return:
        """
        pass
