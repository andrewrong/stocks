class GreptimeDBConfig:
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password


class DuckdbConfig:
    def __init__(self, db_file: str):
        self.db_file = db_file


class DataSource:
    def __init__(self, grep_time_cfg: GreptimeDBConfig, duckdb_cfg: DuckdbConfig, pg_cfg: 'PGConfig'):
        self.grep_time_cfg = grep_time_cfg
        self.duckdb_cfg = duckdb_cfg
        self.pg_cfg = pg_cfg


class PGConfig:
    def __init__(self, host: str, port: int, user: str, password: str, db: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
