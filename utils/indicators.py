from typing import List
import pandas as pd
from ta.volume import VolumeWeightedAveragePrice


def rsi(price: List[float], period: float) -> List[float]:
    """
    Calculate the Relative Strength Index (RSI).
    
    :param price: List of price data.
    :param period: The period over which to calculate RSI.
    :return: A list representing the RSI values.
    From: https://medium.com/@redeaddiscolll/trading-with-time-series-machine-learning-2b6995a8183e
    """
    price_difference = price.diff()
    
    # Separate the gains and losses from the price difference
    gains = price_difference.clamp_min(0).abs()
    losses = price_difference.clamp_max(0).abs()
    
    # Calculate the relative strength (RS)
    rs = gains.ewm(alpha=1 / period).mean() / losses.ewm(alpha=1 / period).mean()
    
    return 100 * (1 - (1 + rs) ** -1)

def macd(price: List[float], fast: float, slow: float, signal_span: float) -> List[float]:
    """
    Calculate the Moving Average Convergence Divergence (MACD) and its signal line.
    
    :param price: List of price data.
    :param fast: The period for the fast EMA.
    :param slow: The period for the slow EMA.
    :param signal_span: The period for the signal line.
    :return: A list representing the MACD signal values.
    From: https://medium.com/@redeaddiscolll/trading-with-time-series-machine-learning-2b6995a8183e
    """
    fast_ema = price.ewm(span=fast, adjust=False).mean()
    slow_ema = price.ewm(span=slow, adjust=False).mean()
    macd_line = fast_ema - slow_ema
    
    # Subtract the EMA of the MACD line to get the signal line
    signal_line = macd_line - macd_line.ewm(span=signal_span, adjust=False).mean()
    
    return signal_line

def vwap(data: pd.DataFrame) -> pd.DataFrame:
    data['TP'] = (data['High'] + data['Low'] + data['Close']) / 3
    data['PV'] = data['TP'] * data['Volume']
    data['CumulativePV'] = data['PV'].cumsum()
    data['CumulativeVolume'] = data['Volume'].cumsum()
    data['VWAP'] = data['CumulativePV'] / data['CumulativeVolume']
    return data

from ta.volume import VolumeWeightedAveragePrice

# ...
def vwap2(df, label='vwap', window=3, fillna=True):
        df[label] = VolumeWeightedAveragePrice(high=df['high'], low=df['low'], close=df["close"], volume=df['volume'], window=window, fillna=fillna).volume_weighted_average_price()
        return df
