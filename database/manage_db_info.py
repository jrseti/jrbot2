import os
import sys
import time
import asyncio
import argparse
import requests
import json
import MySQLdb as mdb
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from processes.processes import Processes
from logger.logger import Logger
from ts_auth0 import TS_Auth
from config import API_KEY, API_SECRET_KEY
from config import API_BASE_URL, API_TICKER_INFO_URI
from config import DB_HOST, DB_USER, DB_PASS, DB_NAME
import pprint

def getDb():
    """Get the database connection
    Returns:
        mdb.connect: The database connection
    """
    try:
        return mdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)
    except Exception as e:
        print(e)
        raise("Cannot access database")

def closeDb(db):
    """Close the database connection
    Args:
        db (mdb.connect): The database connection
    Returns:
        None
    """
    try:
        db.close()
    except Exception as e:
        print(e)
        
    
def get_ticker_details(ticker: str) -> str:
    """Get the ticker info
    Args:
        ticker (str): The ticker
        access_token (str): The access token
    Returns:
        str: The ticker info
    """
    query = f"SELECT * from symbol where ticker = '{ticker}'"
    dbcon = getDb()
    cursor = dbcon.cursor()
    cursor.execute(query)
    """result = cursor.fetchone()
    for row in result:
        print(row)"""
     
    try:   
        col_names = [i[0] for i in cursor.description]
        row = dict(zip(col_names, cursor.fetchone()))
        s = ""
        for col_name in col_names:
            #print(f"{col_name.upper():>18}: {row[col_name]}")
            s += f"{col_name.upper():>18}: {row[col_name]}\n"
    except Exception as e:
        #print(e)
        # Note: adding extra space to be removed during the return.
        s = f"{ticker} not found in database table 'symbol' "
    
    closeDb(dbcon)
    
    # Chop off the last newline. If not found, the trailing space is removed.
    return s[:-1]

def add_symbol(ticker: str) -> None:
    """Add the ticker to the database
    Args:
        ticker (str): The ticker
    Returns:
        None
    """
    print(f"Adding {ticker} to the database")
    
    ts = TS_Auth(API_KEY, API_SECRET_KEY)
    ts.start_auth0()
    access_token = ts.get_access_token()
    
    url = f"{API_TICKER_INFO_URI}/{ticker}"
    headers = {'Authorization': f'Bearer {access_token}' }
    response = requests.request("GET", url, headers=headers)
    json_data = response.json()
    print(json_data)

    # open the database connection
    con = getDb()
    
    
    closeDb(con)
    pass

def delete_symbol(ticker: str) -> None:
    """Delete the ticker from the database
    Args:
        ticker (str): The ticker
    Returns:
        None
    """
    pass

def list_exchanges() -> list:
    """List the excanges available in the database
    Returns:
        str: The list of exchanges
    """
    con = getDb()
    
    query = f"SELECT * from exchanges"
    dbcon = getDb()
    cursor = dbcon.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    exchanges = []
    if rows is not None:
        for row in rows:
            exchanges.append(row[1])
            
    closeDb(con)
    return exchanges
    

if __name__ == "__main__":
   
    parser = argparse.ArgumentParser(description='Adds and deletes ticker info to/from the database.')

    parser.add_argument('ticker')
    parser.add_argument('--info', action='store_true', help='Show ticker info in database')
    parser.add_argument('--add', action='store_true', help='Add the ticker to the database')
    parser.add_argument('--delete', action='store_true', help='Delete the ticker from the database')
    parser.add_argument('--listex', action='store_true', help='List the exchanges. NOTE: need to supply a dummy ticker name.')
    args = parser.parse_args()
    
    if args.add and args.delete:
        print("You can only add or delete a ticker, not both")
        sys.exit(1)
        
    if args.info:
        print(get_ticker_details(args.ticker))
        
    if args.add:
        add_symbol(args.ticker)
        
    if args.delete:
        delete_symbol(args.ticker)
        
    if args.listex:
        print(list_exchanges())
    