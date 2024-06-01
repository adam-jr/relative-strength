#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
import json
import os
from datetime import date
from scipy.stats import linregress
import yaml
from rs_data import cfg, read_json
from functools import reduce

DIR = os.path.dirname(os.path.realpath(__file__))

pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_columns", None)

try:
    with open("config.yaml", "r") as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
    print(exc)

PRICE_DATA = os.path.join(DIR, "data", "price_history.json")
MIN_PERCENTILE = cfg("MIN_PERCENTILE")
POS_COUNT_TARGET = cfg("POSITIONS_COUNT_TARGET")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_TICKERS = "Tickers"
TITLE_SECTOR = "Sector"
TITLE_INDUSTRY = "Industry"
TITLE_MCAP = "Market Cap"
TITLE_UNIVERSE = "Universe" if not ALL_STOCKS else "Exchange"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"

if not os.path.exists("output"):
    os.makedirs("output")


def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    rs = int(rs * 100) / 100  # round to 2 decimals
    return rs


def strength(closes: pd.Series):
    """Calculates the performance of the last year (most recent quarter is weighted double)"""
    try:
        quarters1 = quarters_perf(closes, 1)
        quarters2 = quarters_perf(closes, 2)
        quarters3 = quarters_perf(closes, 3)
        quarters4 = quarters_perf(closes, 4)
        return 0.4 * quarters1 + 0.2 * quarters2 + 0.2 * quarters3 + 0.2 * quarters4
    except:
        return 0


def quarters_perf(closes: pd.Series, n):
    if len(closes) < (n * int(252 / 4)):
        return 0

    start_price = closes.iloc[-n * int(252 / 4)]  # Price 'n' quarters ago
    end_price = closes.iloc[-1]  # Price at the end of the period
    return (end_price / start_price) - 1  # Percentage change in price over the period


def rankings():
    """Returns a dataframe with percentile rankings for relative strength including a column for market capitalization"""
    json = read_json(PRICE_DATA)
    relative_strengths = []
    ranks = []
    stock_rs = {}
    ref = json[REFERENCE_TICKER]
    for ticker in json:
        try:
            closes = list(map(lambda candle: candle["close"], json[ticker]["candles"]))
            closes_ref = list(map(lambda candle: candle["close"], ref["candles"]))
            industry = (
                TICKER_INFO_DICT[ticker]["info"]["industry"]
                if json[ticker]["industry"] == "unknown"
                else json[ticker]["industry"]
            )
            sector = (
                TICKER_INFO_DICT[ticker]["info"]["sector"]
                if json[ticker]["sector"] == "unknown"
                else json[ticker]["sector"]
            )
            market_cap = (
                TICKER_INFO_DICT[ticker]["info"]["marketCap"]
                if "marketCap" in TICKER_INFO_DICT[ticker]["info"]
                else "n/a"
            )  # Assuming market cap data is available in TICKER_INFO_DICT
            if (
                len(closes) >= 6 * 20
                and industry != "n/a"
                and market_cap != "n/a"
                and len(industry.strip()) > 0
                and int(market_cap) > 300_000_000
                and closes[-1] > 10
            ):
                closes_series = pd.Series(closes)
                closes_ref_series = pd.Series(closes_ref)
                rs = relative_strength(closes_series, closes_ref_series)
                month = 20
                tmp_percentile = 100
                rs1m = relative_strength(
                    closes_series.head(-1 * month), closes_ref_series.head(-1 * month)
                )
                rs3m = relative_strength(
                    closes_series.head(-3 * month), closes_ref_series.head(-3 * month)
                )
                rs6m = relative_strength(
                    closes_series.head(-6 * month), closes_ref_series.head(-6 * month)
                )

                # if rs is too big assume there is faulty price data
                if rs < 12_000:
                    # stocks output
                    ranks.append(len(ranks) + 1)
                    relative_strengths.append(
                        (
                            0,
                            ticker,
                            sector,
                            industry,
                            json[ticker]["universe"],
                            rs,
                            tmp_percentile,
                            rs1m,
                            rs3m,
                            rs6m,
                            market_cap,
                            closes[-1],
                        )
                    )  # Include market cap in the tuple
                    stock_rs[ticker] = rs
        except KeyError:
            print(f"Ticker {ticker} has invalid data")
    dfs = []
    suffix = ""

    # stocks
    df = pd.DataFrame(
        relative_strengths,
        columns=[
            TITLE_RANK,
            TITLE_TICKER,
            TITLE_SECTOR,
            TITLE_INDUSTRY,
            TITLE_UNIVERSE,
            TITLE_RS,
            TITLE_PERCENTILE,
            TITLE_1M,
            TITLE_3M,
            TITLE_6M,
            "Market Cap",
            "Close",
        ],
    )  # Include "Market Cap" column in columns list
    df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
    df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
    df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
    df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
    df = df.sort_values(([TITLE_RS]), ascending=False)
    df[TITLE_RANK] = ranks
    out_tickers_count = 0
    for index, row in df.iterrows():
        if row[TITLE_PERCENTILE] >= MIN_PERCENTILE:
            out_tickers_count = out_tickers_count + 1
    df = df.head(out_tickers_count)

    df.to_csv(
        os.path.join(DIR, "output", f'rs_stocks_{date.today().strftime("%Y%m%d")}.csv'),
        index=False,
    )
    dfs.append(df)

    return dfs


def main(skipEnter=False):
    ranks = rankings()
    print(ranks[0])
    print("***\nYour 'rs_stocks.csv' is in the output folder.\n***")
    if not skipEnter and cfg("EXIT_WAIT_FOR_ENTER"):
        input("Press Enter key to exit...")


if __name__ == "__main__":
    main()
