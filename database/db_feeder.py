#!/home/jrseti/jrbot2/jrbot2_venv/bin/python
import os
import sys
from datetime import datetime as dt
from datetime import timedelta
import requests
import MySQLdb as mdb
from ts_auth0 import TS_Auth
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import API_BASE_URL, API_BARCHARTS_URI
from config import API_KEY, API_SECRET_KEY
from config import DB_HOST, DB_USER, DB_PASS, DB_NAME
from utils.datetime_utils import *

#from ..config import *

def toDb(db_conn, ticker, query_reqults):

    values = []
    for bar in query_reqults['Bars']:
        #print(bar)
        if(bar['BarStatus'] != 'Closed'):
            continue
        bar_values = []
        bar_values.append(ticker)
        bar_values.append(bar['TimeStamp'].replace('T',' ').replace('Z', ''))
        bar_values.append(bar['Open'])
        bar_values.append(bar['High'])
        bar_values.append(bar['Low'])
        bar_values.append(bar['Close'])
        bar_values.append(bar['TotalVolume'])
        bar_values.append(bar['OpenInterest'])
        values.append(bar_values)

    # Create the insert strings
    column_str = (
        "ticker, datetime, open, high, "
        "low, close, volume, open_interest"
    )

    insert_str = ("%s, " * 8)[:-2]
    final_str = "INSERT INTO bars_1min (%s) VALUES (%s)" % \
        (column_str, insert_str)
    
    #print(values)
    #print(final_str)

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    cur = db_conn.cursor()
    result = cur.executemany(final_str, values)
    if result is not None:
        print(f"{ticker}: Inserted {result} rows")
    db_conn.commit()

def to_dbase_since_last(db_conn, access_token, ticker):
    """Get the bars for the given ticker and time period. Puth them in the database
    Args:
        access_token (str): The access token for the API
        ticker (str): The ticker to get the bars for
        start_date (str): The start date for the bars
        end_date (str): The end date for the bars
    """
    query = f"select datetime from bars_1min where ticker='{ticker}' order by datetime DESC LIMIT 1"
    cur = db_conn.cursor()
    cur.execute(query)

    myresult = cur.fetchall()
    # add 1 minute to the last datetime
    last_dt = myresult[0][0]
    start_date = last_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    #print(start_date)
    end_date = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    get_bars(access_token, ticker, start_date, end_date)

    


def get_bars(access_token, ticker, start_date, end_date):
    """Get the bars for the given ticker and time period. Puth them in the database
    Args:
        access_token (str): The access token for the API
        ticker (str): The ticker to get the bars for
        start_date (str): The start date in the format YYYY-MM-DDTHH:MM:SSZ
        end_date (str): The end date in the format YYYY-MM-DDTHH:MM:SSZ
    Returns:
        None
    """
    # create a datetime object from the start
    start_dt = dt.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    # loop through the days until the end
    #print(f"End date: {end_date}")
    while start_dt <= dt.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ"):

        #print("Start of while loop")
        start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = start_dt.strftime("%Y-%m-%dT23:59:59Z")
        if end > dt.now().strftime("%Y-%m-%dT%H:%M:%SZ"):
            end = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        #print(start, end)

        url = f"{API_BASE_URL.replace('sim-','')}/{API_BARCHARTS_URI}/{ticker}"
        url += f"?interval=1&unit=Minute&firstdate={start}&lastdate={end}"
        print(url)
        headers = {'Authorization': f'Bearer {access_token}' }

        # try on timeout exception
        try:
            response = requests.request("GET", url, headers=headers, timeout=5)
        except requests.exceptions.Timeout:
            print(f"Timeout getting bars for {ticker} on {start} - {end}")
            return
        if response.status_code != 200:

            print(f"Error {response.status_code} getting bars for {ticker} on {start} - {end}")
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            #time.sleep(1)
            #print(response.status_code)
            #print(response.text)
            
        else:
            print(f"Success getting bars for {ticker} on {start} - {end}")
            #time.sleep(1)
            json_data = response.json()
            num_bars = len(json_data['Bars'])
            #print(num_bars)
            #print(json_data['Bars'])
            toDb(con, ticker, json_data)

            # add a day
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            

        #print(f"end of while loop, start_dt={start_dt}")

print("Starting")
# open the database connection
db_host = DB_HOST
db_user = DB_USER
db_pass = DB_PASS
db_name = DB_NAME
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

ts = TS_Auth(API_KEY, API_SECRET_KEY)
ts.start_auth0()

access_token = ts.get_access_token()

#TICKER = "RTYU23"
TICKERS = ['RTYU23', 'SPY','$DJX.X']
#TICKERS = ['RTYU23']
TICKERS = [ 'RTYU23','SPY', 'ESU23']

start_date = "2023-06-24T00:00:00Z"
#start_date = "2023-01-01T00:00:00Z"
#start_date = "2023-07-11T00:00:00Z"
end_date = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
#get_bars(access_token, TICKER, start_date, end_date)
for ticker in TICKERS:
    #get_bars(access_token, ticker, start_date, end_date)
    to_dbase_since_last(con, access_token, ticker)


   
