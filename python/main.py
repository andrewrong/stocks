import json
import logging
import time
from datetime import datetime, timedelta
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler

import compute.indicator
from common import common
from datasource.ddb import DuckDBClient
from datasource.pg import PgClient
from flask import Flask, request, jsonify, g
from functools import wraps
import jwt

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=logging.INFO, format=log_format, datefmt=date_format)


class Config:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        self.stocks = []
        self.import_history_date = config['history_import_date']
        self.cron_time = config['cron_time']
        self.data_source_cfg = config['data_source']
        self.http_token = config['http_token']

        duckdb_client = DuckDBClient(config['data_source']['duckdb_cfg']['db_file'])
        try:
            self.load_config_from_db(duckdb_client)
        finally:
            duckdb_client.close()

    def load_config_from_db(self, duckdb_client):
        query = "SELECT * FROM stock_info"
        stock_data = duckdb_client.execute(query).fetchall()
        for row in stock_data:
            if not row[4]:
                logging.info("Invalid stock: %s", row['symbol'])
                continue
            self.stocks.append(
                common.StockInfo(row[0], row[1], common.StockType.from_string(row[2]),
                                 row[3]))
            logging.info("Loaded stock: %s", row[0])


def main():
    config = Config('config.json')
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

    # app = create_http_app(dbClient, config.http_token)
    # app.run(host='0.0.0.0', port=4501, debug=True)


# 写一个导入历史数据的函数
def import_history_data(duckClient: common.DbClient, pgClient: common.DbClient, stocks: list[common.StockInfo]):
    for stock in stocks:
        logging.info(f"Fetching stock data for {stock.symbol}")
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
        start = result[0].strftime('%Y-%m-%d') if result[0] else (datetime.now() - timedelta(days=2)).strftime(
            '%Y-%m-%d')
        end = datetime.now().strftime('%Y-%m-%d')

        logging.info(f"Fetching stock data for {stock.symbol} from {start} to {end}")

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


# def create_http_app(duckdb_client: common.DbClient, token: str) -> Flask:
#     app = Flask(__name__)
#     app.config['SECRET_KEY'] = token
#     app.config['duck_client'] = duckdb_client
#
#     @app.before_request
#     def before_request():
#         # 每个请求开始时将客户端赋值给 g 对象
#         if 'db' not in g:
#             g.db = app.config['duck_client']
#
#     def token_required(f):
#         @wraps(f)
#         def decorated(*args, **kwargs):
#             token = request.headers.get('Authorization')
#             if not token:
#                 return jsonify({'message': 'Token is missing'}), 403
#             try:
#                 data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
#             except:
#                 return jsonify({'message': 'Token is invalid'}), 403
#             return f(*args, **kwargs)
#
#         return decorated
#
#     @app.route('/stock', methods=['POST'])
#     @token_required
#     def create_stock():
#         data = request.get_json()
#         infos = [(data['symbol'], data['name'], data['type'], data['currency'], True)]
#         # Save new_stock to database
#         g.db.batch_insert_stockinfo(infos)
#         return jsonify({'message': 'New stock created'}), 201
#
#     @app.route('/stock/<symbol>', methods=['GET'])
#     @token_required
#     def get_stock(symbol):
#         stock = g.db.execute("SELECT * FROM stock_info WHERE symbol = ?", (symbol,)).fetchone()
#         if not stock:
#             return jsonify({'message': 'Stock not found'}), 404
#         return jsonify(stock.__dict__), 200
#
#     @app.route('/stock/<symbol>', methods=['PUT'])
#     @token_required
#     def update_stock(symbol):
#         data = request.get_json()
#         # Update stock in database
#         infos = [(data['symbol'], data['name'], data['type'], data['currency'], True)]
#         # Save new_stock to database
#         g.db.batch_insert_stockinfo(infos)
#
#         return jsonify({'message': 'Stock updated'}), 200
#
#     @app.route('/stock/<symbol>', methods=['DELETE'])
#     @token_required
#     def delete_stock(symbol):
#         data = g.db.execute("select * from stock_info where symbol = ?", (symbol,)).fetchone()
#         if not data:
#             return jsonify({'message': 'Stock not found'}), 404
#         data['valid'] = False
#         g.db.batch_update([(data['symbol'], data['name'], data['type'], data['currency'], data['valid'])])
#         return jsonify({'message': 'Stock deleted'}), 200
#
#     return app


if __name__ == "__main__":
    main()
