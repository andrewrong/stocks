package datasource

import (
	"database/sql"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/markcheno/go-quote"

	"stock_data/common"
)

// DuckDBClient 封装 DuckDB 操作的结构体
type DuckDBClient struct {
	db *sql.DB
}

// NewDuckDBClient 创建一个新的 DuckDB 客户端
func NewDuckDBClient(dbFilePath string) (*DuckDBClient, error) {
	db, err := sql.Open("duckdb", dbFilePath)
	if err != nil {
		return nil, err
	}
	// 创建表（如果不存在）
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS stock_prices (
		ts TIMESTAMP,
		symbol VARCHAR,
		open_price DOUBLE,
		high DOUBLE,
		low DOUBLE,
		close_price DOUBLE,
		volume DOUBLE,
		currency VARCHAR,
		stock_name VARCHAR,
		stock_type VARCHAR,
		PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)
	)`)
	if err != nil {
		return nil, err
	}
	return &DuckDBClient{db: db}, nil
}

// Close 关闭数据库连接
func (client *DuckDBClient) Close() error {
	return client.db.Close()
}

// BatchInsert 批量插入数据
func (client *DuckDBClient) batchInsert(data []*common.StockPrice) error {
	tx, err := client.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO stock_prices 
		(ts, symbol, open_price, high, low, close_price, volume, currency, stock_name, stock_type) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT
		DO UPDATE SET open_price = EXCLUDED.open_price, high = EXCLUDED.high, low = EXCLUDED.low, close_price = EXCLUDED.close_price, volume = EXCLUDED.volume`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, record := range data {
		_, err = stmt.Exec(record.TS, record.Symbol, record.OpenPrice, record.High, record.Low, record.ClosePrice, record.Volume, record.Currency, record.StockName, record.StockType)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (c *DuckDBClient) BatchInsert(stockData *quote.Quote, stockInfo *common.StockInfo) error {
	stockPrices := common.QuoteDataToStockPrices(stockData, stockInfo)
	return c.batchInsert(stockPrices)
}

func (c *DuckDBClient) Type() string {
	return "duckdb"
}
