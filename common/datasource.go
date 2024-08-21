package common

// GreptimeDBConfig 结构体表示GreptimeDB的配置
type GreptimeDBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type DuckdbConfig struct {
	DbFile string `json:"db_file"`
}

type DataSource struct {
	GrepTimeCfg *GreptimeDBConfig `json:"grep_time_cfg"`
	DuckdbCfg   *DuckdbConfig     `json:"duckdb_cfg"`
	PGCfg       *PGConfig         `json:"pg_cfg"`
}

type PGConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
	Db   string `json:"db"`
}
