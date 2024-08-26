import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler

import compute.indicator
from common import common
from datasource.ddb import DuckDBClient
from datasource.pg import PgClient

logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        self.stocks = []

        for stock in config['stocks']:
            self.stocks.append(
                common.StockInfo(stock['symbol'], stock['name'], common.StockType.from_string(stock['type']),
                                 stock['currency']))

        self.import_history_date = config['history_import_date']
        self.cron_time = config['cron_time']
        self.data_source_cfg = config['data_source']


def main():
    config = Config('config.json')
    dbClient: common.DbClient
    # pgClient: common.DbClient
    dbClient = DuckDBClient(config.data_source_cfg['duckdb_cfg']['db_file'])
    # pgClient = PgClient(config.data_source_cfg['pg_cfg'])

    clients = [dbClient]

    start_date = datetime.strptime(config.import_history_date, '%Y-%m-%d')
    history = start_date.timestamp() * 1000

    # scheduler = BackgroundScheduler()
    # scheduler.add_job(func=lambda: fetch_and_store_stock_data(clients, config.stocks, history), trigger='cron',
    #                   **config.cron_time)
    # scheduler.start()

    # try:
    #     while True:
    #         time.sleep(3600)
    #         logging.info("Running...")
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()
    import_history_data(clients, config.stocks)


# 写一个导入历史数据的函数
def import_history_data(clients: list[common.DbClient], stocks: list[common.StockInfo]):
    for stock in stocks:
        print(f"Fetching stock data for {stock.symbol}")
        start = (datetime.now() - timedelta(days=100 * 365)).strftime('%Y-%m-%d')
        end = datetime.now().strftime('%Y-%m-%d')

        stock_data = yf.download(stock.symbol, start=start, end=end, interval='1d')
        if stock_data.empty:
            logging.warning(f"No data fetched for {stock.symbol}")
            continue

        data = []
        for index, row in stock_data.iterrows():
            data.append((index, stock.symbol, row['Open'], row['High'], row['Low'], row['Close'], row["Adj Close"],
                         row['Volume'],
                         stock.currency, stock.name,
                         common.StockType.string(stock.s_type)))
        duckClient = clients[0]
        duckClient.batch_insert(data)
        logging.info(f"Stock data for [{duckClient.type()}] {stock.symbol} inserted successfully!")
        # pgClient.batch_insert(data)
        # logging.info(f"Stock data for [{pgClient.type()}] {stock.symbol} inserted successfully!")

        calculate_sma_with_duckdb(duckClient, stock, start, end)


def fetch_and_store_stock_data(clients, stocks: list[common.StockInfo], history):
    for stock in stocks:
        start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        end = datetime.now().strftime('%Y-%m-%d')

        if datetime.now().timestamp() * 1000 < history:
            logging.info(
                f"Skipping fetch and store stock data for {stock.symbol}, start: {start}, history start: {datetime.fromtimestamp(history / 1000).strftime('%Y-%m-%d')}")
            continue

        stock_data = yf.download(stock.symbol, start=start, end=end, interval='1d')
        if stock_data.empty:
            logging.warning(f"No data fetched for {stock.symbol}")
            continue

        data = []
        for index, row in stock_data.iterrows():
            data.append((index, stock.symbol, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'],
                         stock.currency, stock.name,
                         common.StockType.string(stock.s_type)))
        for client in clients:
            client.batch_insert(data)
            logging.info(f"Stock data for [{client.type()}] {stock.symbol} inserted successfully!")


def calculate_sma_with_duckdb(client: common.DbClient, stock: common.StockInfo, start: str,
                              end: str):
    result = compute.indicator.calc_multi_indicator(client, stock, start, end)
    data = []
    for k_ts, v in result["data"].items():
        data.append((
            v["sma_5"],
            v["sma_20"],
            v["sma_50"],
            v["sma_120"],
            v["sma_200"],
            v["ema_5"],
            v["ema_20"],
            v["ema_50"],
            v["ema_120"],
            v["ema_200"],
            v["macd"]["macd"],
            v["macd"]["macd_signal"],
            v["macd"]['macd_hist'],
            v["bbands"]["upper"],
            v["bbands"]["middle"],
            v["bbands"]["lower"],
            v['rsi_7'],
            v['rsi_14'],
            v['rsi_28'],
            result["symbol"],
            k_ts,
        ))
    client.batch_update(data)

def duckdb_to_pg(duckClient: common.DbClient, pgClient: common.DbClient, start: str, end: str):
    query = "select * from stock_prices where ts >= '{}' and ts <= '{}' and adj_close is not null and adj_close != 0".format(start, end)
    duckdb_data = duckClient.execute(query).fetchall()

    data = []



if __name__ == "__main__":
    main()
