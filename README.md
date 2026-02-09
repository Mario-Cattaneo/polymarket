This repository is part of the thesis for Polymarkets Hybrid Exchange System and Prediction Market Arbitrage.

It consists of several subdirectories, some of which are deprecated clients due to incompatible API changes.

1) scripts:
This directory contains two bash scripts used to setup and terminate the postgres 15 daemon server process used for all data collection and analysis scripts.

2) kalshi_collection (Deprecated):
This was an initial exchange wide data collecting client, which is now reimplemented in events, but tailored for specific events and data collection.

3) orderbook_collection (Deprecated):
This was an initial exchange wide data collecting client, which is now reimplemented in events, but tailored for specific events and data collection.

4) polygon_collection: 
Contain the main collect_events.py script for collecting the events from the smart contracts deployed on polygon. Also contians other polygon related scripts.

5) events:
Has scripts for fetching historical polymarket market objects, processing and analyzing them.

6) misc_collectors:
Scripts for testing or quick remedies.

7) target_events:
Contains the scripts which collect event specific data from Polymarket and Kalshi.

8) agreggation:
Has scripts for aggregating large and dense tables into analytic significant data for faster analytics.

9) data_analysis:
Contains scripts for analyzing the collected data.
