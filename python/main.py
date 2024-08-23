import json
import time
import logging
from datetime import datetime, timedelta
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler

import compute.indicator
from common import common
from datasource.pg import PgClient
from datasource.ddb import DuckDBClient

logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        self.stocks = config['stocks']
        self.data_source_cfg = config['data_source']
        self.import_history_date = config['history_import_date']
        self.cron_time = config['cron_time']


def main():
    config = Config('config.json')

    duckdb_client = DuckDBClient(config.data_source_cfg['duckdb_cfg']['db_file'])
    pg_client = PgClient(config.data_source_cfg['pg_cfg'])

    clients = [pg_client, duckdb_client]

    start_date = datetime.strptime(config.import_history_date, '%Y-%m-%d')
    history = start_date.timestamp() * 1000

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=lambda: fetch_and_store_stock_data(clients, config.stocks, history), trigger='cron',
                      **config.cron_time)
    scheduler.start()

    try:
        while True:
            time.sleep(3600)
            logging.info("Running...")
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


def fetch_and_store_stock_data(clients, stocks, history):
    for stock in stocks:
        start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        end = datetime.now().strftime('%Y-%m-%d')

        if datetime.now().timestamp() * 1000 < history:
            logging.info(
                f"Skipping fetch and store stock data for {stock['symbol']}, start: {start}, history start: {datetime.fromtimestamp(history / 1000).strftime('%Y-%m-%d')}")
            continue

        stock_data = yf.download(stock['symbol'], start=start, end=end, interval='1d')
        if stock_data.empty:
            logging.warning(f"No data fetched for {stock['symbol']}")
            continue

        data = []
        for index, row in stock_data.iterrows():
            data.append((index, stock['symbol'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'],
                         stock['currency'], stock['name'],
                         stock['type']))
        for client in clients:
            client.batch_insert(data)
            logging.info(f"Stock data for [{client.type()}] {stock['symbol']} inserted successfully!")


def calculate_sma(client: common.DbClient, stock: common.StockInfo):
    sma_results = compute.indicator.multi_sma(client, stock)
    data = []
    for i in range(len(sma_results[0])):
        data.append((
            sma_results[0][i],  # sma5
            sma_results[1][i],  # sma20
            sma_results[2][i],  # sma50
            sma_results[3][i],  # sma120
            sma_results[4][i],  # sma250
            stock.symbol,
            sma_results[0].index[i]  # ts
        ))
    client.batch_update(data)

        if __name__ == "__main__":
            main()
