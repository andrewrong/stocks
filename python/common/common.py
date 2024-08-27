from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any


class StockPrice:
    def __init__(self, ts: datetime, symbol: str, open_price: float, high: float, low: float, close_price: float, adj_close : float,
                 volume: float, currency: str, stock_name:
            str, stock_type: str):
        self.ts = ts
        self.symbol = symbol
        self.open_price = open_price
        self.high = high
        self.low = low
        self.close_price = close_price
        self.adj_close = adj_close
        self.volume = volume
        self.currency = currency
        self.stock_name = stock_name
        self.stock_type = stock_type

class StockType:
    INDEX = "INDEX"
    STOCK = "STOCK"
    CRYPTO = "CRYPTO"
    OTHER = "OTHER"

    @staticmethod
    def string(stock_type):
        return stock_type

    @classmethod
    def from_string(cls, stock_type_str):
        stock_type_map = {
            "INDEX": cls.INDEX,
            "STOCK": cls.STOCK,
            "CRYPTO": cls.CRYPTO,
            "OTHER": cls.OTHER
        }
        return stock_type_map.get(stock_type_str)


class StockInfo:
    def __init__(self, symbol: str, name: str, s_type: StockType, currency: str):
        self.symbol = symbol
        self.name = name
        self.s_type = s_type
        self.currency = currency


def quote_data_to_stock_prices(data: Dict[str, Any], stock_info: StockInfo) -> List[StockPrice]:
    if not data:
        return []

    stock_prices = []
    for i in range(len(data['Close'])):
        stock_prices.append(StockPrice(
            ts=data['Date'][i],
            symbol=stock_info.symbol,
            open_price=data['Open'][i],
            high=data['High'][i],
            low=data['Low'][i],
            close_price=data['Close'][i],
            volume=data['Volume'][i],
            currency=stock_info.currency,
            stock_name=stock_info.name,
            stock_type=stock_info.s_type.string(stock_info.s_type)
        ))
    return stock_prices


class StockDataGP:
    def __init__(self, symbol: str, ts: datetime, open: float, high: float, low: float, close: float, volume: float,
                 currency: str, s_type: str, name: str):
        self.symbol = symbol
        self.ts = ts
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.currency = currency
        self.s_type = s_type
        self.name = name

    def table_name(self) -> str:
        return "stock_prices"


def quote_data_to_gp_row(stock_data: Dict[str, Any], stock_info: StockInfo) -> List[StockDataGP]:
    if not stock_data or not stock_data['Close']:
        return []

    rows = []
    for i in range(len(stock_data['Close'])):
        rows.append(StockDataGP(
            symbol=stock_data['Symbol'],
            ts=stock_data['Date'][i],
            open=stock_data['Open'][i],
            high=stock_data['High'][i],
            low=stock_data['Low'][i],
            close=stock_data['Close'][i],
            volume=stock_data['Volume'][i],
            currency=stock_info.currency,
            s_type=stock_info.s_type.string(stock_info.s_type),
            name=stock_info.name
        ))
    return rows


class DbClient(ABC):
    @abstractmethod
    def batch_insert(self, data: List[Any]) -> None:
        pass

    @abstractmethod
    def batch_update(self, data: List[Any]) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def type(self) -> str:
        pass

    @abstractmethod
    def execute(self, query):
        pass
