package common

import (
	"time"

	"github.com/markcheno/go-quote"
)

// StockPrice 表示股票价格的结构体
type StockPrice struct {
	TS         time.Time
	Symbol     string
	OpenPrice  float64
	High       float64
	Low        float64
	ClosePrice float64
	Volume     float64
	Currency   string
	StockName  string
	StockType  string
}

type StockType string

func (s StockType) String() string {
	return string(s)
}

const (
	StockTypeIndex StockType = "INDEX"
	StockTypeStock StockType = "STOCK"
	// 虚拟币
	StockTypeCrypto StockType = "CRYPTO"
	StockTypeOther  StockType = "OTHER"
)

type StockInfo struct {
	Symbol   string    `json:"symbol"`
	Name     string    `json:"name"`
	SType    StockType `json:"type"`
	Currency string    `json:"currency"`
}

func QuoteDataToStockPrices(data *quote.Quote, stockInfo *StockInfo) []*StockPrice {
	if data == nil {
		return nil
	}
	// stockPrices := make([]*StockPrice, 0)
	stockPrices := make([]*StockPrice, len(data.Close))
	for i := 0; i < len(data.Close); i++ {
		stockPrices[i] = &StockPrice{
			TS:         data.Date[i],
			Symbol:     stockInfo.Symbol,
			OpenPrice:  data.Open[i],
			High:       data.High[i],
			Low:        data.Low[i],
			ClosePrice: data.Close[i],
			Volume:     data.Volume[i],
			Currency:   stockInfo.Currency,
			StockName:  stockInfo.Name,
			StockType:  stockInfo.SType.String(),
		}
	}
	return stockPrices
}

type StockDataGP struct {
	Symbol   string    `greptime:"tag;column:symbol;type:string"`
	Ts       time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
	Open     float64   `greptime:"field;column:open_price;type:float64"`
	High     float64   `greptime:"field;column:high;type:float64"`
	Low      float64   `greptime:"field;column:low;type:float64"`
	Close    float64   `greptime:"field;column:close_price;type:float64"`
	Volume   float64   `greptime:"field;column:volume;type:float64"`
	Currency string    `greptime:"tag;column:currency;type:string"`
	SType    string    `greptime:"tag;column:stock_type;type:string"`
	Name     string    `greptime:"tag;column:stock_name;type:string"`
}

func (s StockDataGP) TableName() string {
	return "stock_prices"
}

// StockDataToGPRow
func QuoteDataToGPRow(stockData *quote.Quote, stockInfo *StockInfo) []StockDataGP {
	if stockData == nil {
		return nil
	}

	if len(stockData.Close) == 0 {
		return nil
	}
	row := make([]StockDataGP, 0)

	for i := 0; i < len(stockData.Close); i++ {
		tmp := StockDataGP{
			Symbol:   stockData.Symbol,
			Ts:       stockData.Date[i],
			Open:     stockData.Open[i],
			High:     stockData.High[i],
			Low:      stockData.Low[i],
			Close:    stockData.Close[i],
			Volume:   stockData.Volume[i],
			Currency: stockInfo.Currency,
			SType:    stockInfo.SType.String(),
			Name:     stockInfo.Name,
		}

		row = append(row, tmp)
	}
	return row
}

type DbClient interface {
	BatchInsert(stockData *quote.Quote, stockInfo *StockInfo) error
	Close() error
	Type() string
}
