from talib import abstract
import talib

import common.common

SMA = abstract.Function("sma")
BBANDS = abstract.Function("bbands")
EMA = abstract.Function("ema")


def sma(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str, period=30) -> dict:
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    return {"sma": SMA(dataframes['adj_close'], timeperiod=period), "ts": dataframes['ts']}


def multi_sma(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str,
              periods=None) -> dict:
    if periods is None:
        periods = [5, 20, 50, 120, 200]
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    return {"sma": [SMA(dataframes['adj_close'], timeperiod=period) for period in periods], "ts": dataframes['ts']}


def sma5(duckConn, stock, start: str, end: str) -> dict:
    return sma(duckConn, stock, start, end, 5)


def sma20(duckConn, stock, start: str, end: str) -> dict:
    return sma(duckConn, stock, start, end, 20)


def sma50(duckConn, stock, start: str, end: str) -> dict:
    return sma(duckConn, stock, start, end, 60)


def sma120(duckConn, stock, start: str, end: str) -> dict:
    return sma(duckConn, stock, start, end, 120)


def sma250(duckConn, stock, start: str, end: str) -> dict:
    return sma(duckConn, stock, start, end, 250)


# 这个指标：Bollinger Bands， 用来检测股票价格的离散程度， 与其它的技术指标不同的是，它不是基于价格的，而是基于价格的波动。
def bbands(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    upper, middle, lower = BBANDS(dataframes['adj_close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    return {"bbands": {"upper": upper, "middle": middle, "lower": lower}, "ts": dataframes['ts']}


# 新增的 ema 函数, 这个指标类似于sma，不同的在于对最近的点的权重会更加大一些，会更加敏感，适合做短期交易和趋势分析
def ema(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str, period=30) -> dict:
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    return {"ema": EMA(dataframes['adj_close'], timeperiod=period), "ts": dataframes['ts']}


# 不同周期的 ema 函数
def multi_ema(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str,
              periods=None) -> dict:
    if periods is None:
        periods = [5, 20, 50, 120, 250]
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    return {"ema": [EMA(dataframes['adj_close'], timeperiod=period) for period in periods], "ts": dataframes['ts']}



def ema5(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    return ema(client, stock, start, end, 5)


def ema20(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    return ema(client, stock, start, end, 20)


def ema50(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    return ema(client, stock, start, end, 50)


def ema120(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    return ema(client, stock, start, end, 120)


def ema250(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    return ema(client, stock, start, end, 250)

def macd(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str) -> dict:
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    macd, macd_signal, macd_hist = talib.MACD(dataframes['adj_close'])
    return {"macd": macd, "macd_signal": macd_signal, "macd_hist": macd_hist, "ts": dataframes['ts']}

def rsi(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str, period=14) -> dict:
    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()
    rsi = talib.RSI(dataframes['adj_close'], timeperiod=period)
    return {"rsi": rsi, "ts": dataframes['ts']}
