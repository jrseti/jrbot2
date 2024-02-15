import sys
import os
import subprocess
import json
import argparse
import datetime as dt
from utils.bars import Bar
import email_trades
from config import EMAIL_RECIPIENTS

BARS_DIR = "bars"
bar_count = 0
max_secs = -1
last_bar = None
bars_closed = []
bars = []
bars_current_one_minute = []
num_trades = 0
current_trade = None
total_profit = 0
total_trades = 0

BRACKET_UP = 2.0
BRACKET_DOWN = 4.0

def is_now(datetime):
    """Determine if the datetime is within 5 seconds of now.
    Args:
        datetime (datetime): The datetime to check
    Returns:
        bool: True if the datetime is within 5 seconds of now, False otherwise
    """
    return datetime >= dt.datetime.now() + dt.timedelta(seconds=-5)

def process(line):
    """Process each line tailed from the file.
    Args:
        line (str): The line, which is a JSON string
    Returns:
        None
    """
    global bar_count
    bar_count += 1
    
    # Convert the line to a JSON object
    j = json.loads(line)
    
    close = float(j['Close'])
    volume = int(j['TotalVolume'])
    bar_status = j['BarStatus']
    datetime = dt.datetime.strptime(j['time_received'], '%Y-%m-%d %H:%M:%S.%f')
    is_current = is_now(datetime)
    
    if is_current is False:
        print(f'*{bar_count}* ', f'{datetime} {close:.2f} {volume}', )
    else:
        print(f'[{bar_count}] ', f'{datetime} {close:.2f} {volume}' )
        
    if bar_status == 'Closed':
        print('===========================')
        
def get_close_bar_minute_gaps(line):
    """Get the gaps between the close of one bar and the open of the next.
    Args:
        line (str): The line, which is a JSON string
    Returns:
        None
    """
    global bar_count
    bar_count += 1
    global max_secs
    global last_bar
    global total_profit
    
    bar = Bar(line)
    if bar.is_open == False:
        if last_bar != None:
            #print(bar.get_diff_seconds(last_bar))
            temp_max_secs = max(max_secs, bar.get_diff_seconds(last_bar))
            if temp_max_secs != max_secs:
                print(f'Max seconds between bars: {temp_max_secs}, {bar.datetime}')
            if bar.get_diff_seconds(last_bar) > 70:
                print(f'*** {bar.datetime} {bar.get_diff_seconds(last_bar)}***')
            max_secs = temp_max_secs
        last_bar = bar

def main(ticker, date):
    
    print(f"Retrieving data for {ticker} on {date}")
    
    # Create the filename
    filename = os.path.join(BARS_DIR, date, f"{ticker}.log")
    if os.path.exists(filename) is False:
        print(f"File {filename} does not exist")
        return
    f = subprocess.Popen(['tail','-F', '-n 100000', filename],\
        stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    while True:
        line = f.stdout.readline()
        #get_close_bar_minute_gaps(line)
        bar = Bar(line)
        strategy1(bar)
        
class Trade:
    
    def __init__(self, bar: Bar, direction: int):
        self.bar = bar
        self.direction = direction
        
    def should_sell(self, bar: Bar):
        # If the direction is 1, we are looking for a sell
        # Sell if the bar is up 2 points or mor, or down 4 ponints or more.
        if self.direction == 1:
            if bar.close - self.bar.close >= BRACKET_UP or self.bar.close - bar.close >= BRACKET_DOWN:
                return True
        return False
    
    def should_buy(self, bar: Bar):
        # If the direction is -1, we are looking for a buy
        # Buy if the bar is down 2 points or more, or up 4 points or more.
        if self.direction == -1:
            if self.bar.close - bar.close >= BRACKET_UP or bar.close - self.bar.close >= BRACKET_DOWN:
                return True
        return False
    
    def profit(self, current_bar: Bar) -> float:
        """Calculate the profit of the trade
        Args:
            current_bar (Bar): The current bar
        Returns:
            float: The profit of the trade
        """
        if self.direction == 1:
            return current_bar.close - self.bar.close
        else:
            return self.bar.close - current_bar.close
        
        
def strategy1(bar: Bar):
    """First try at a strategy
    Args:
        bar (Bar): The bar to process
    Returns:
        None
    """
    global bars_closed
    global bars
    global num_trades
    global bars_current_one_minute
    global current_trade
    global total_profit
    global total_trades
    
    # Only during the trading day!
    if not bar.is_during_day():
        return
    
    # If closed, add to the list of closed bars
    if bar.is_open == False:
        bars_closed.append(bar)
        if current_trade != None:
            print("\n")
        print(f"Bars closed {bar.get_local_time()}, volume: ({bar.up_volume-bar.down_ticks}), {bar.down_volume}, {bar.up_volume} [{total_profit}:{num_trades}]")
        bars_current_one_minute = []
        return
    
    # If the bar is not closed, add it to the list of current one-minute bars
    bars_current_one_minute.append(bar)
    
    if current_trade != None:
        if current_trade.should_sell(bar):
            print(f"\n**SELL {bar.get_local_time()}, close: {bar.close}, profit: {current_trade.profit(bar)}")
            total_profit += current_trade.profit(bar)
            
            if is_now(bar.datetime):
                for recipient in EMAIL_RECIPIENTS:
                    email_trades.send_email(recipient, f"SELL", f"**SELL  {bar.get_local_time()}, close: {bar.close}, total_profit: {total_profit}", None)


            print(f"Total profit: {total_profit}")
            current_trade = None
        """elif current_trade.should_buy(bar):
            print(f"\n**BUY {bar.get_local_time()}, close: {bar.close}, profit: {current_trade.profit(bar)}")
            total_profit += current_trade.profit(bar)
            print(f"Total profit: {total_profit}")
            current_trade = None"""
        if current_trade != None:
            print(f"{current_trade.profit(bar)}" , end=",", flush=True)
        return
    
    if Bar.are_previous_up_bars(bars_closed, 2):
        if Bar.are_last_n_ticks_up(bars_current_one_minute, 3):
            num_trades += 1
            print(f"**BUY  {bar.get_local_time()}, close: {bar.close}")
            
            if is_now(bar.datetime):
                for recipient in EMAIL_RECIPIENTS:
                    email_trades.send_email(recipient, f"Buy", f"**BUY  {bar.get_local_time()}, close: {bar.close}", None)
            current_trade = Trade(bar, 1)
            """print("BARS CLOSED")
            for bar in bars_closed[-2:]:
                print(f"{bar.datetime} {bar.open} {bar.close}")
            print("ONE_MINUTES")
            last = None
            for bar in bars_current_one_minute[-3:]:
                print(f"{bar.datetime} {bar.close}")"""
            #sys.exit(0)
            """print(f"Number of trades: {num_trades}")
            print(f"Bars closed: {len(bars_closed)}")
            print(f"TRADE {num_trades} {bar.datetime}")"""
    """if Bar.are_previous_down_bars(bars_closed, 2):
        if Bar.are_last_n_ticks_down(bars_current_one_minute, 3):
            num_trades += 1
            print(f"**SELL  {bar.get_local_time()}, close: {bar.close}")
            current_trade = Trade(bar, -1)"""
        
    #print(f"Bars closed: {len(bars_closed)}, num_trades: {num_trades}")
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Retrieves data from TradeStation and sends it to a redis-stream.')

    parser.add_argument('ticker')
    parser.add_argument('date')
    
    ticker = parser.parse_args().ticker
    date = parser.parse_args().date
    
    main(ticker, date)  # Call the main function
    

    
    