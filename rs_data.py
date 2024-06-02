#!/usr/bin/env python
import requests
import json
import time
import bs4 as bs
import datetime as dt
import os
import pandas_datareader.data as web
import pickle
import requests
import yaml
import yfinance as yf
import pandas as pd
import dateutil.relativedelta
import numpy as np
import re
import rs_nasdaq_securities
from ftplib import FTP
from io import StringIO
from time import sleep

from datetime import date
from datetime import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(DIR, "data")):
    os.makedirs(os.path.join(DIR, "data"))
if not os.path.exists(os.path.join(DIR, "tmp")):
    os.makedirs(os.path.join(DIR, "tmp"))

try:
    with open(os.path.join(DIR, "config_private.yaml"), "r") as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
    print(exc)

try:
    with open("config.yaml", "r") as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
    print(exc)


def cfg(key):
    try:
        return private_config[key]
    except:
        try:
            return config[key]
        except:
            return None


def read_json(json_file):
    with open(json_file, "r", encoding="utf-8") as fp:
        return json.load(fp)


def write_to_file(dict, file):
    with open(file, "w", encoding="utf8") as fp:
        json.dump(dict, fp, ensure_ascii=False)


PRICE_DATA_FILE = os.path.join(DIR, "data", "price_history.json")


def write_price_history_file(tickers_dict):
    write_to_file(tickers_dict, PRICE_DATA_FILE)


def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["industry"] = security["industry"]
    ticker_response["universe"] = security["universe"]


def print_data_progress(
    ticker, universe, idx, securities, error_text, elapsed_s, remaining_s
):
    dt_ref = datetime.fromtimestamp(0)
    dt_e = datetime.fromtimestamp(elapsed_s)
    elapsed = dateutil.relativedelta.relativedelta(dt_e, dt_ref)
    if remaining_s and not np.isnan(remaining_s):
        dt_r = datetime.fromtimestamp(remaining_s)
        remaining = dateutil.relativedelta.relativedelta(dt_r, dt_ref)
        remaining_string = (
            f"{remaining.hours}h {remaining.minutes}m {remaining.seconds}s"
        )
    else:
        remaining_string = "?"
    print(
        f"{ticker} from {universe}{error_text} ({idx+1} / {len(securities)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}."
    )


def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = (
        pd.Series(all_load_times).rolling(np.minimum(idx + 1, 25)).mean().tail(1).item()
    )
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds


def escape_ticker(ticker):
    return ticker.replace(".", "-")


def get_yf_data(security, start_date, end_date):
    ticker_data = {}
    ticker = security["ticker"]
    escaped_ticker = escape_ticker(ticker)
    df = yf.download(escaped_ticker, start=start_date, end=end_date, auto_adjust=True)
    yahoo_response = df.to_dict()
    timestamps = list(yahoo_response["Open"].keys())
    timestamps = list(map(lambda timestamp: int(timestamp.timestamp()), timestamps))
    opens = list(yahoo_response["Open"].values())
    closes = list(yahoo_response["Close"].values())
    lows = list(yahoo_response["Low"].values())
    highs = list(yahoo_response["High"].values())
    volumes = list(yahoo_response["Volume"].values())
    candles = []

    for i in range(0, len(opens)):
        candle = {}
        candle["open"] = opens[i]
        candle["close"] = closes[i]
        candle["low"] = lows[i]
        candle["high"] = highs[i]
        candle["volume"] = volumes[i]
        candle["datetime"] = timestamps[i]
        candles.append(candle)

    ticker_data["candles"] = candles
    enrich_ticker_data(ticker_data, security)
    return ticker_data


def load_prices_from_yahoo():
    print("*** Loading Stocks from Yahoo Finance ***")
    today = date.today() + dt.timedelta(days=1)
    start = time.time()
    start_date = today - dt.timedelta(days=365 + 183)  # 183 = 6 months
    tickers_dict = {}
    load_times = []

    securities = rs_nasdaq_securities.get_resolved_securities().values()
    for idx, security in enumerate(securities):
        load_price_history(security, tickers_dict, start_date, today)
        track_progress(load_times, start, time.time(), idx, security, securities)

    write_price_history_file(tickers_dict)


def load_price_history(security, tickers_dict, start_date, end_date):
    ticker = security["ticker"]
    ticker_data = get_yf_data(security, start_date, end_date)
    tickers_dict[ticker] = ticker_data


def track_progress(load_times, start, r_start, idx, security, securities):
    now = time.time()
    current_load_time = now - r_start
    load_times.append(current_load_time)
    remaining_seconds = remaining_seconds = get_remaining_seconds(
        load_times, idx, len(securities)
    )
    print_data_progress(
        security["ticker"],
        security["universe"],
        idx,
        securities,
        "",
        time.time() - start,
        remaining_seconds,
    )


def main():
    load_prices_from_yahoo()


if __name__ == "__main__":
    main()
