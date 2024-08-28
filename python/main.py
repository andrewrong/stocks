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
    def __init__(self, duckdb_client):
        self.stocks = []
        self.import_history_date = None
        self.cron_time = None
        self.data_source_cfg = None
        self.load_config_from_db(duckdb_client)

    def load_config_from_db(self, duckdb_client):
        query = "SELECT * FROM config"
        config_data = duckdb_client.execute(query).fetchall()
        for row in config_data:
            if row[0] == 'stocks':
                for stock in row[1]:
                    self.stocks.append(
                        common.StockInfo(stock['symbol'], stock['name'], common.StockType.from_string(stock['type']),
                                         stock['currency']))
            elif row[0] == 'history_import_date':
                self.import_history_date = row[1]
            elif row[0] == 'cron_time':
                self.cron_time = row[1]
            elif row[0] == 'data_source':
                self.data_source_cfg = row[1]


def main():
    config = Config('config.json')
    dbClient: common.DbClient
    # pgClient: common.DbClient
    dbClient = DuckDBClient(config.data_source_cfg['duckdb_cfg']['db_file'])
    pgClient = PgClient(config.data_source_cfg['pg_cfg'])

    start_date = datetime.strptime(config.import_history_date, '%Y-%m-%d')
    history = start_date.timestamp() * 1000

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=lambda: fetch_and_store_stock_data(dbClient, pgClient, config.stocks, history),
                      trigger='cron',
                      **config.cron_time)
    scheduler.start()

    try:
        while True:
            time.sleep(3600)
            logging.info("Running...")
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

    # start = (datetime.now() - timedelta(days=100 * 365)).strftime('%Y-%m-%d')
    # end = datetime.now().strftime('%Y-%m-%d')
    # for stock in config.stocks:
    #     duckdb_to_pg(dbClient, pgClient, stock, start, end)


# 写一个导入历史数据的函数
def import_history_data(duckClient: common.DbClient, pgClient: common.DbClient, stocks: list[common.StockInfo]):
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
        duckClient.batch_insert(data)
        logging.info(f"Stock data for [{duckClient.type()}] {stock.symbol} inserted successfully!")

        # step 2
        calculate_indicator_with_duckdb(duckClient, stock, start, end)

        # step 3
        # duckdb_to_pg(duckClient, pgClient, start, end)


def fetch_and_store_stock_data(client: common.DbClient, pgClient: common.DbClient, stocks: list[common.StockInfo]):
    for stock in stocks:
        query = f"SELECT MAX(ts) FROM stock_prices WHERE symbol = '{stock.symbol}'"
        result = client.execute(query).fetchone()
        start = result[0].strftime('%Y-%m-%d') if result[0] else (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        end = datetime.now().strftime('%Y-%m-%d')

        stock_data = yf.download(stock.symbol, start=start, end=end, interval='1d')
        if stock_data.empty:
            logging.warning(f"No data fetched for {stock.symbol}")
            continue

        data = []
        for index, row in stock_data.iterrows():
            data.append((index, stock.symbol, row['Open'], row['High'], row['Low'], row['Close'], row["Adj Close"], row['Volume'],
                         stock.currency, stock.name,
                         common.StockType.string(stock.s_type)))
        client.batch_insert(data)
        logging.info(f"Stock data for [{client.type()}] {stock.symbol} inserted successfully!")

        # step 2
        logging.info(f"calc indicator with duckdb, start:{start}, end:{end}")
        calculate_indicator_with_duckdb(client, stock, start, end)

        # step 3
        logging.info(f"convert to pg, start:{start}, end:{end}")
        duckdb_to_pg(client, pgClient, stock, start, end)


def calculate_indicator_with_duckdb(client: common.DbClient, stock: common.StockInfo, start: str,
                                    end: str):
    logging.info(f"calc indicator with duckdb, start:{start}, end:{end}")
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


def duckdb_to_pg(duckClient: common.DbClient, pgClient: common.DbClient, stock: common.StockInfo, start: str, end: str):
    logging.info(f"duckdb to pg, start: {start}, end: {end}, symbol:{stock.symbol}")
    query = "select * from stock_prices where ts >= '{}' and ts <= '{}' and symbol = '{}' and adj_close is not null " \
            "and adj_close != 0".format(start, end, stock.symbol)
    duckdb_data = duckClient.execute(query).fetchall()

    data = []
    for row in duckdb_data:
        data.append((
            row[0],  # ts
            row[1],  # symbol
            row[2],  # open_price
            row[3],  # high
            row[4],  # low
            row[5],  # close_price
            row[6],  # adj_close
            row[7],  # volume
            row[8],  # currency
            row[9],  # stock_name
            row[10],  # stock_type
            row[11],  # sma5
            row[12],  # sma20
            row[13],  # sma50
            row[14],  # sma120
            row[15],  # sma200
            row[16],  # ema5
            row[17],  # ema20
            row[18],  # ema50
            row[19],  # ema120
            row[20],  # ema200
            row[21],  # macd
            row[22],  # macd_signal
            row[23],  # macd_hist
            row[24],  # rsi7
            row[25],  # rsi14
            row[26],  # rsi28
            row[27],  # bb_upper
            row[28],  # bb_middle
            row[29],  # bb_lower
        ))

    pgClient.batch_insert(data)
    logging.info(f"Stock data for [{pgClient.type()}] {stock.symbol} inserted successfully!")


if __name__ == "__main__":
    main()
