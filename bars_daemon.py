from io import TextIOWrapper
import os
import sys
from datetime import datetime as dt
from datetime import timedelta
import requests
import asyncio
import json
import MySQLdb as mdb
import numpy as np
from logger.logger import Logger
from processes.processes import Processes
from scipy.stats import linregress
from ts_auth0 import TS_Auth
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from utils.datetime_utils import *
from config import API_KEY, API_SECRET_KEY
from config import API_BASE_URL, API_STREAM_BARCHARTS_URI

LOG_NAME = "bars_d"
BARS_DIR = "bars"
REQUEST_TIMEOUT = 10.0

BAR_DOJI = "----"
BAR_UP = u'\u2191'
BAR_DOWN = u'\u2193'
BAR_UP = ' UP '
BAR_DOWN = 'DOWN'

async def loop(ts: TS_Auth, tickers: list, logger: Logger) -> None:
    """Create and start the tasks for each ticker.
    Args:
        ts (TS_Auth): The TS_Auth object
        tickers (list): The list of tickers
        logger (Logger): The logger object
    Returns:
        None
    """

    logger.info(f"Starting loop for {tickers}")
    tasks = []
    for ticker in tickers:
        tasks.append(stream_bars(ts, ticker, logger))
    await asyncio.gather(*tasks)
    
def main(args):
    """Kicks off starting all the tasks and waits for them to finish.
    Args:
        args (dict): a dict containing the comma separated tickers 
                    passed to the script
                    {'tickers' : "ticker1,ticker2,..."}
    Returns:
        None
    """
    logger = Logger(LOG_NAME)
    
    # Make sure we have tickers to work with
    if 'tickers' not in args.keys():
        print("No tickers passed to script, exiting.")
        logger.error("No tickers passed to script, exiting.")
        return

    # Parse the tickers int a list
    tickers = args['tickers'].split(',')
    logger.info(f"Starting for {tickers}")
    print(f"Starting for {tickers}")

    # Authenticate with TradeStation
    ts = TS_Auth(API_KEY, API_SECRET_KEY) 
    ts.start_auth0()

    run_loop = asyncio.get_event_loop()
    #for ticker in tickers:
    #logger.info(f"Starting task for {ticker}")
    run_loop.run_until_complete(loop(ts, tickers, logger))
    run_loop.close()

def get_file(ticker: str, 
             current_filename: str, 
             current_filepointer: TextIOWrapper) -> tuple:
    """Create a filename and open a file pointer for the bar data.
    Args:
        ticker (str): The ticker symbol
        current_filename (str): The current filename
    Returns:
        tuple: The filename and file pointer
    """
    date = dt.now().strftime("%Y%m%d")
    filename = os.path.join(BARS_DIR, date, f"{ticker}.log")
    if filename != current_filename:
        if current_filepointer is not None:
            current_filepointer.close()
        new_dir = os.path.join(BARS_DIR, date)
        if not os.path.isdir(new_dir):
            os.mkdir(new_dir)
        filename = os.path.join(new_dir, f"{ticker}.log")
        return filename, open(filename, 'a')
    return current_filename, current_filepointer

async def stream_bars(ts: TS_Auth, ticker: str, logger: Logger) -> None:
    """Stream bars from tradestation
    Args:
    """

    barsback = 10
    MAX_TREND_PRICES = 100
    STD_NUM = 20
    price_list = []
    index_list = []
    count = 0
    bar_open = 0
    close = 0
    log_filename = ""
    fp = None
    
    logger.info(f"Starting stream_bars for {ticker}")
    
    await asyncio.sleep(0.1)
    
    while True:
        try:
            access_token = ts.get_access_token()
            url = f"{API_BASE_URL}/{API_STREAM_BARCHARTS_URI}/"
            url += f"{ticker}?interval=1&unit=minute&barsback={barsback}"
            headers = {'Authorization': f'Bearer {access_token}'}
            print(url)
            print(f"Header = {headers}")
            
            logger.info(f"\n\n\nBar streaming URL = {url}")
            
            print("\n\n\n\n")

            bar_direction = BAR_DOJI
            waiting_for_open = False

            # Set up the request with a timeout
            try:
                res = requests.get(url = url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                await asyncio.sleep(REQUEST_TIMEOUT)
                print("REQUEST TIMEOUT!")
                continue
            
            if res.status_code != 200: 
                #print(f'Status code from bar stream for {ticker}: {res.status_code}')
                barsback = 10
                await asyncio.sleep(0)
                continue
            for line in res.iter_lines():
                #await asyncio.sleep(0)
                #logger.info(f"Ticker: {ticker} Line: {line}")
                print(f"Ticker: {ticker} Line: {line}")
                if line:
                    if line is None or len(line) == 0:
                        continue

                    bar_json = json.loads(line)

                    if 'IsRealtime' not in bar_json:
                        continue
                    if bar_json['IsRealtime'] == False:
                        waiting_for_open = True
                        continue
                     
                    if bar_json['IsRealtime'] == False:
                        continue
                    if 'Heartbeat' in bar_json:
                        continue
                      
                    bar_json['time_received'] = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    del bar_json['Epoch']
                    del bar_json['IsEndOfHistory']
                    del bar_json['OpenInterest']
                    del bar_json['IsRealtime']
                    
                    # Get the file pointer and write the bar data
                    log_filename, fp = get_file(ticker, log_filename, fp)
                    logger.info(f"Writing bar to {log_filename}")
                    fp.write(f"{json.dumps(bar_json)}\n")
                    fp.flush()
                    
                #
                await asyncio.sleep(0)
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Exception in stream_bars: {e}")
            logger.error(f"Exception in stream_bars: {e}")
            await asyncio.sleep(0)
            continue
                    
                    
                    
        
if __name__ == "__main__":
    ts = TS_Auth(API_KEY, API_SECRET_KEY)
    ts.start_auth0()

    access_token = ts.get_access_token()
    
    tickers = 'ESH24,NQH24,YMH24'
    #tickers = 'NQH24,YMH24'
    
    #tickers = sys.argv[1]
    tickers = 'ESH24'
    
    task_args = {'tickers' : tickers}
    
    
    #main(task_args)
    Processes.daemonize(main, task_args, os.path.basename(__file__), os.getcwd(), False)