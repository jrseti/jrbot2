import os
import sys
import time
import asyncio
import argparse
import requests
import json
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from processes.processes import Processes
from logger.logger import Logger
from ts_auth0 import TS_Auth
from config import API_KEY, API_SECRET_KEY
from config import API_BASE_URL, API_STREAM_BARCHARTS_URI

"""
This script streams bar data from TradeStation and sends it to a redis-stream.
Command line arguments alow for starting the script as a daemon or not.
"""

LOG_NAME = "trade_station_data_d"

def _bar_to_redis_stream(ticker: str, bar: dict, logger: Logger) -> None:
    """Send the bar to the redis stream.
    Args:
        ticker (str): The ticker
        bar (dict): The bar
        logger (Logger): The logger object
    Returns:
        None
    """
    logger.info(f"Ticker: {ticker} Bar: {bar}")
    pass

async def stream_bars(ts: TS_Auth, ticker: str, logger: Logger) -> None:
    """Stream bars for the given ticker.
    Args:
        ts (TS_Auth): The TS_Auth object
        ticker (str): The ticker
        logger (Logger): The logger object
    Returns:
        None
    """
    barsback = 10

    try:
        while True:
            count = 0
            await asyncio.sleep(1)

            access_token = ts.get_access_token()

            url = f"{API_BASE_URL}/{API_STREAM_BARCHARTS_URI}/"
            url += f"{ticker}?interval=1&unit=minute&barsback={barsback}"
            logger.info(f"{ticker} URL: {url}")
            headers = {'Authorization': f'Bearer {access_token}'}
            count += 1
            logger.info(f"Ticker: {ticker} Count: {count}")

            res = requests.get(url = url, headers=headers, stream=True)
            if res.status_code != 200: 
                logger.warn(f'Status code from bar stream for {ticker}: {res.status_code}')
                barsback = 10
                continue
            for line in res.iter_lines():
                await asyncio.sleep(0)
                if line:
                    logger.info(f"Ticker: {ticker} Count: {count} Line: {line}")
                    bar_json = json.loads(line)
                    _bar_to_redis_stream(ticker, bar_json, logger)
                    count += 1
    except Exception as ex:
        logger.error(f"Exception in stream_bars for {ticker}: {ex}")
        await asyncio.sleep(5)
        barsback = 10
 
    logger.warn(f"Exiting stream_bars for {ticker}")
    

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
        args (dict): a dict containing the comma separaterd tickers 
                    passed to the script
    Returns:
        None
    """
    logger = Logger(LOG_NAME)

    tickers = args['tickers'].split(',')
    logger.info(f"Starting for {args['tickers']}")

    ts = TS_Auth(API_KEY, API_SECRET_KEY) 
    ts.start_auth0()

    run_loop = asyncio.get_event_loop()
    run_loop.run_until_complete(loop(ts, tickers, logger))
    run_loop.close()

if __name__ == "__main__":
   
    parser = argparse.ArgumentParser(description='Retrieves data from TradeStation and sends it to a redis-stream.')

    parser.add_argument('tickers')
    parser.add_argument('-n', '--no_daemon', action='store_true', help='Do not \
                            make this process a daemon. Defaults to turning \
                            this process into a daemon.')
    
    tickers = parser.parse_args().tickers
    task_args = {'tickers' : tickers}

    args = parser.parse_args()
    if args.no_daemon:
        main(task_args)
    else:
        Processes.daemonize(main, task_args, os.path.basename(__file__), os.getcwd(), True)
