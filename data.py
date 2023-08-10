# data.py
import os
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import pandas as pd
from IPython.display import display
#import MySQLdb as mdb
import sqlalchemy
import gc
import requests
import asyncio
from config import *
import queue
from urllib.parse import quote_plus
import json

from event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    def __init__(self, events, ticker_list, max_rows=10000):
        """
        Initialises the data handler by requesting the location of the
        CSV files and a list of symbols.

        It will be assumed that all files are of the form 'symbol.csv',
        where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        ticker_list - A list of symbol strings.
        """
        self.events = events
        self.ticker_list = ticker_list
        self.continue_backtest = True  
        self.max_rows = max_rows     
        self.latest_data = {}

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low, 
        close, volume, open interest).
        Note that this is an 
        """
        pass

    def getDf(self):
        return self.df


class RealtimeData(DataHandler):
    """Class for handling realtime data from tradestation."""

    def __init__(self, events, ticker_list, max_rows=10000):
        super(RealtimeData, self).__init__(events, ticker_list, max_rows)
        self.df = None
        self.continue_backtest = False
        self.latest_data = {}
        
    async def stream(self, ticker, ts):
        """Stream bars from tradestation"""
        barsback = 1
        while True:
            try:
                access_token = ts.get_access_token()
                url = f"{API_BASE_URL}/{API_STREAM_BARCHARTS_URI}/"
                ulr += f"{ticker}?interval=1&unit=minute&barsback={barsback}"
                headers = {'Authorization': f'Bearer {access_token}'}
                print(url)
                print(f"Header = {headers}")
                res = requests.get(url = url, headers=headers, stream=True)
                if res.status_code != 200: 
                    #print(f'Status code from bar stream for {ticker}: {res.status_code}')
                    barsback = 10
                    continue
                for line in res.iter_lines():
                    await asyncio.sleep(0)
                    if line:
                        if line is None or len(line) == 0:
                            continue
                        print(line)
                        bar_json = json.loads(line)
                        print(bar_json)
                        event = MarketEvent(ticker)
                        self.events.put(event)
                        """bar = Bar(ticker, bar_json)
                        if bar.isValid == True:
                            #print(bar)

                            self.events.put(bar)
                        else:
                            if bar.isHeartbeat() is True:
                                #print(f"Heartbeat: {ticker}")
                                self.events.put(bar)
                            else:
                                print(f"Invalid bar: {ticker} {bar_json}")"""
                       
            
            except Exception as ex:
                print(f'{ticker}: Sleeping 5 seconds, will try to reconnect again.')
                print(ex)
                await asyncio.sleep(5)
                barsback = 10
                pass

    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")

  
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        pass

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        pass

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        pass

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        pass


    def create_stream_tasks(self, ts):
        """Create tasks for streaming bars from tradestation.
        Args:
            ts: Tradestation object
        Returns:
            tasks: list of tasks
        """
        tasks = []
        for ticker in self.ticker_list:
            tasks.append(self.stream(ticker, ts))
        return tasks
        


class HistoricalDbData(DataHandler):
    """Class for handling historic data from a MySQL database."""

    def __init__(self, events, ticker_list, start_date, end_date, max_rows=10000):
        """
        Initialises the historic data handler by requesting
        a list of symbols.

        Parameters:
        events - The Event Queue.
        ticker_list - A list of ticker strings.
        start_date - The start date of the historical data.
        end_date - The end date of the historical data.
        max_rows - The maximum number of rows keep in latest list.
        """

        super(HistoricalDbData, self).__init__(events, ticker_list, max_rows)

        self.start_date = start_date
        self.end_date = end_date
        self.db_data = {}

        # Create the database engine
        url = f'mysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        #print(url)
        self.engine = sqlalchemy.create_engine(url)
    
    def _get_new_bar(self, ticker):
        """Gets the latest bar from the data read from the database."""
        for b in self.db_data[ticker]:
            yield b

    def update_bars(self):
        """
        Pushes the latest bar to the latest_data structure
        for all symbols in the symbol list.
        This simulates a live bar stream from a brokerage.
        """
        for s in self.ticker_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_data[s].append(bar)
                    if(len(self.latest_data[s]) > self.max_rows):
                        self.latest_data[s].pop(0)
        self.events.put(MarketEvent())

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]
        
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
        
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])
        
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([b[0] for b in bars_list])
        
    def get_latest_bars_datetimes(self, symbol, N=1):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N][0]
        
    def read_from_dbase(self):
        # Read the data from the database on row at a time starting with self.start_date

        for ticker in self.ticker_list:
            query  = f"SELECT * FROM bars_1min WHERE ticker='{ticker}' AND "
            query += f"datetime >= '{self.start_date}' AND "
            query += f"datetime <= '{self.end_date}' "
            query += "ORDER BY datetime ASC"
            #print(query)
            #self.cursor.execute(query)
            #df = pd.read_sql(query, con=self.db_con)
            

            with self.engine.connect() as conn:
                self.db_data[ticker] = pd.read_sql(query, con = conn)
                self.db_data[ticker].set_index('datetime', inplace=True)
                self.db_data[ticker].drop(['id', 'ticker'], axis=1, inplace=True)
                self.db_data[ticker] = self.db_data[ticker].iterrows()

            # Set the latest data to Empty for this ticker
            self.latest_data[ticker] = []

if __name__ == "__main__":
    events = queue.Queue()
    """h_db = HistoricalDbData(events, ['RTY'], '2019-02-01', '2019-02-02')
    h_db.read_from_dbase()
    h_db.update_bars()
    h_db.update_bars()
    h_db.update_bars()
    bars = h_db.get_latest_bars('RTY', 4)
    print(bars)
    print(f"NUM Events in Queue: {events.qsize()}")

    vals = h_db.get_latest_bars_values('RTY', 'close', 5)
    print(vals)
    val = h_db.get_latest_bar_value('RTY', 'close')
    print(val)
    last_dt = h_db.get_latest_bar_datetime('RTY')
    print(last_dt)
    last_dts = h_db.get_latest_bars_datetimes('RTY', 1)
    print(f"LST DTS={last_dts}")"""
    