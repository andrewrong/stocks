package datasource

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/markcheno/go-quote"

	"stock_data/common"
)

// PgClient 封装 PostgreSQL 操作的结构体
type PgClient struct {
	db *sql.DB
}

// NewPgClient 创建一个新的 PostgreSQL 客户端
func NewPgClient(cfg *common.PGConfig) (*PgClient, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Pass, cfg.Db)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	// 创建表（如果不存在）
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS stock_prices (                                                                                                                                                                                                                 
         ts TIMESTAMP,                                                                                                                                                                                                                                                           
         symbol VARCHAR(50),                                                                                                                                                                                                                                                         
         open_price DOUBLE PRECISION,                                                                                                                                                                                                                                            
         high DOUBLE PRECISION,                                                                                                                                                                                                                                                  
         low DOUBLE PRECISION,                                                                                                                                                                                                                                                   
         close_price DOUBLE PRECISION,                                                                                                                                                                                                                                           
         volume DOUBLE PRECISION,                                                                                                                                                                                                                                                
         currency VARCHAR(10),                                                                                                                                                                                                                                                       
         stock_name VARCHAR(1024),                                                                                                                                                                                                                                                     
         stock_type VARCHAR(50),                                                                                                                                                                                                                                                     
         PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)                                                                                                                                                                                                              
     )`)
	if err != nil {
		return nil, err
	}
	return &PgClient{db: db}, nil
}

// Close 关闭数据库连接
func (client *PgClient) Close() error {
	return client.db.Close()
}

// batchInsert 批量插入数据
func (client *PgClient) batchInsert(data []*common.StockPrice) error {
	tx, err := client.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO stock_prices                                                                                                                                                                                                                           
         (ts, symbol, open_price, high, low, close_price, volume, currency, stock_name, stock_type)                                                                                                                                                                              
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (symbol, currency, stock_name, stock_type, ts)
		 DO UPDATE SET open_price = $3, high = $4, low = $5, close_price = $6, volume = $7`)
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

// BatchInsert 批量插入数据
func (client *PgClient) BatchInsert(stockData *quote.Quote, stockInfo *common.StockInfo) error {
	stockPrices := common.QuoteDataToStockPrices(stockData, stockInfo)
	return client.batchInsert(stockPrices)
}

func (c *PgClient) Type() string {
	return "postgres"
}
