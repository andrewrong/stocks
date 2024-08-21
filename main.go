package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/markcheno/go-quote"

	"stock_data/common"
	"stock_data/datasource"
)

// Config 结构体表示配置文件
type Config struct {
	Stocks            []common.StockInfo `json:"stocks"`
	DataSourceCfg     common.DataSource  `json:"data_source"`
	ImportHistoryDate string             `json:"history_import_date"`
}

func main() {
	// 读取配置文件
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	duckdbClient, err := datasource.NewDuckDBClient(config.DataSourceCfg.DuckdbCfg.DbFile)
	if err != nil {
		log.Fatalf("Failed to create duckdb client: %v", err)
		return
	}
	defer duckdbClient.Close()

	client, err := datasource.NewPgClient(config.DataSourceCfg.PGCfg)
	if err != nil {
		log.Fatalf("Failed to create PG client: %v", err)
		return
	}
	defer duckdbClient.Close()

	// 将历史时间字符串变成time.Time
	startDate, err := time.Parse("2006-01-02", config.ImportHistoryDate)
	if err != nil {
		log.Fatalf("Failed to parse start date: %v", err)
		return
	}
	log.Println("start date:", startDate)

	//history := startDate.UnixMilli()
	//
	//// 创建定时任务
	//c := cron.New()
	//c.AddFunc("0 17 * * * ", func() {
	//	log.Printf("now: %s, i will fetch and store stock data", time.Now().Format("2006-01-02 15:04:05"))
	//	for _, symbol := range config.Stocks {
	//		start := time.Now().Format("2006-01-02")
	//		end := time.Now().Add(24 * time.Hour).Format("2006-01-02")
	//
	//		if time.Now().UnixMilli() < history {
	//			log.Printf("skip fetch and store stock data for %s, start:%s, history start:%s", symbol, start, config.ImportHistoryDate)
	//			continue
	//		}
	//
	//		err := fetchAndStoreStockData(client, symbol, start, end)
	//		if err != nil {
	//			log.Printf("Failed to fetch and store stock data for %s: %v", symbol, err)
	//		}
	//	}
	//})
	//c.Start()

	clients := []common.DbClient{client, duckdbClient}

	// 将历史数据进行import
	err = importHistoryData(clients, config.Stocks)
	if err != nil {
		log.Fatalf("Failed to import history data: %v", err)
	}
	// 保持程序运行
	select {}
}

// 将历史数据进行import
func importHistoryData(clients []common.DbClient, stocks []common.StockInfo) error {
	for _, stock := range stocks {
		if stock.Symbol == "" {
			continue
		}
		log.Printf("Importing stock data for %s", stock)
		// 获得最近100年的数据
		start := time.Now().Add(-100 * 365 * 24 * time.Hour).Format("2006-01-02")
		end := time.Now().Format("2006-01-02")
		err := fetchAndStoreStockData(clients, stock, start, end)
		if err != nil {
			log.Printf("Failed to fetch and store stock data for %s: %v", stock, err)
			return err
		}
		time.Sleep(30 * time.Second)
	}
	return nil
}

// loadConfig 函数读取配置文件
func loadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// fetchAndStoreStockData 函数获取并存储股票数据
func fetchAndStoreStockData(clients []common.DbClient, stock common.StockInfo, start, end string) error {
	stockData, err := quote.NewQuoteFromYahoo(stock.Symbol, start, end, quote.Daily, true)
	if err != nil {
		log.Printf("Failed to fetch stock data for %s: %v", stock.Symbol, err)
		return err
	}

	for _, client := range clients {
		// 存储股票数据
		err = client.BatchInsert(&stockData, &stock)
		if err != nil {
			log.Printf("Failed to write stock data for [%v] %s: %v", client.Type(), stock.Symbol, err)
			continue
		}
		fmt.Printf("Stock data for [%v] %s inserted successfully!\n", client.Type(), stock.Symbol)
	}

	return nil
}
