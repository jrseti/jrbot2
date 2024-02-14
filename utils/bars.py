# Some utilities related to bars
from typing import Union
import json
from collections import namedtuple
from typing import NamedTuple
import datetime

class _Bar(NamedTuple):
    """Private namedtuple class for a bar"""
    datetime: datetime.datetime 
    is_open: bool 
    open: float 
    high: float 
    low: float
    close: float
    total_volume: float
    up_volume: int 
    down_volume: int 
    total_ticks: int 
    up_ticks: int 
    down_ticks: int 
    unchanged_ticks: int 
    unchanged_volume: int

class Bar(_Bar):
    __slots__ = ()  # Prevent creation of a __dict__.
    
    @classmethod
    #def __new__(cls, bar_data: Union[dict, str, bytes, bytearray], **kwargs):
    def __new__(cls, *args, **kwargs):
        """"""
        this_bar = args[1]
        print(this_bar)
        if type(this_bar) != dict:
            this_bar = json.loads(this_bar)
    
        #{"High": "38422", "Low": "38417", "Open": "38417", "Close": "38419", 
        # "TimeStamp": "2024-02-14T15:49:00Z", "TotalVolume": "86", 
        # "DownTicks": 27, "DownVolume": 31, "TotalTicks": 82, 
        # "UnchangedTicks": 0, "UnchangedVolume": 0, 
        # "UpTicks": 55, "UpVolume": 55, 
        # "BarStatus": "Open", 
        # "time_received": "2024-02-14 15:48:22.513444"}
        if 'Heartbeat' in this_bar:
            new_args = cls, None, None, None, None, None, None, None, None, None, None, None, None, None, None
            return super().__new__(*new_args, **kwargs)
        
        #print(args)
        
        new_args = cls, datetime.datetime.strptime(this_bar['time_received'], '%Y-%m-%d %H:%M:%S.%f'),  this_bar['BarStatus'] == 'Open', \
            float(this_bar['Open']), float(this_bar['High']), float(this_bar['Low']), float(this_bar['Close']), \
            int(this_bar['TotalVolume']), int(this_bar['UpVolume']), int(this_bar['DownVolume']), \
            int(this_bar['TotalTicks']), \
            int(this_bar['UpTicks']), int(this_bar['DownTicks']), int(this_bar['UnchangedTicks']), int(this_bar['UnchangedVolume']) 
        return super().__new__(*new_args, **kwargs)
    
    def __str__(self) -> str:
        """Pretty print a string representation of this bar"""
        return f"Bar: {self.datetime} - Open: {self.open} - High: {self.high} - Low: {self.low} - Close: {self.close} - Volume: {self.total_volume}"
       
    def is_up_bar(self) -> bool:
        """Determine if the bar is an up bar"""
        return self.close > self.open
    
def is_bar_up(bar: Bar) -> bool:
    """Determine if the bar is an up bar"""
    return bar.close > bar.open

if __name__ == "__main__":

    # Test the Bar class
    bar_str = '{"High": "38422", "Low": "38417", "Open": "38417", "Close": "38419", "TimeStamp": "2024-02-14T15:49:00Z", "TotalVolume": "86", "DownTicks": 27, "DownVolume": 31, "TotalTicks": 82, "UnchangedTicks": 0, "UnchangedVolume": 0, "UpTicks": 55, "UpVolume": 55, "BarStatus": "Open", "time_received": "2024-02-14 15:48:22.513444"}'
    bar = Bar(bar_str)
    print(bar.close)
    print(bar.datetime)
    print(bar.is_up_bar())
    # Test the jsonBarToNamedTuple function
    bar_json = {"High": "38422", "Low": "38417", "Open": "38417", "Close": "38419", 
                "TimeStamp": "2024-02-14T15:49:00Z", "TotalVolume": "86", 
                "DownTicks": 27, "DownVolume": 31, "TotalTicks": 82, 
                "UnchangedTicks": 0, "UnchangedVolume": 0, 
                "UpTicks": 55, "UpVolume": 55, 
                "BarStatus": "Open", 
                "time_received": "2024-02-14 15:48:22.513444"}
    bar = Bar(bar_json)
    print(bar.close)
    print(bar.datetime)
    print(bar.is_up_bar())
    """
    bar = jsonBarToNamedTuple(bar_json)
    print(bar)
    print(bar.datetime)
    print(bar.is_open)
    print(bar.open)
    print(bar.high)
    print(bar.low)
    print(bar.close)
    print(bar.total_volume)
    print(bar.up_volume)
    print(bar.down_volume)
    print(bar.total_ticks)
    print(bar.up_ticks)
    print(bar.down_ticks)
    print(bar.unchanged_ticks)
    print(bar.unchanged_volume)
    
    bar_str = '{"High": "38422", "Low": "38417", "Open": "38417", "Close": "38419", "TimeStamp": "2024-02-14T15:49:00Z", "TotalVolume": "86", "DownTicks": 27, "DownVolume": 31, "TotalTicks": 82, "UnchangedTicks": 0, "UnchangedVolume": 0, "UpTicks": 55, "UpVolume": 55, "BarStatus": "Open", "time_received": "2024-02-14 15:48:22.513444"}'
    bar = jsonBarToNamedTuple(json.loads(bar_str))"""