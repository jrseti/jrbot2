# Data Retrieval

This folder contains code directly related to retrieving data from a market data source and distributing it to Redis.

## tradestation_streamer_d.py
A daemon that retrieves OHLCV and other data from Tradestation streams and publishes to Redis. This is meant to be running full time int the background (thus, a daemon) and strategies can subscribe to the Redis Stream. Note that this only handles for TraseStation. Other streaming daemon code will be added for other providers.

## data.py
An abstract class allowing access to specific data from providers that are not streamed. Such as current orders and positions, account info, ticker detail, etc.
