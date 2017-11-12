package goava

import (
	"github.com/meooio/goava/driver/cassandradb"
	"github.com/meooio/goava/ops"
)

type CassDBConfig struct {
	ServerList string `toml:"server_list"`
	Port       int    `toml:"port"`
	KeySpace   string `toml:"keyspace"`
}

// just as n example
type MySQLDBConfig struct {
	DBServer     string `toml:"db_server_name"`
	Port         int    `toml:"port"`
	DatabaseName string `toml:"database"`
}

type ClientConfig struct {
	DBType          string        `toml:"dbtype"`
	CassandraConfig CassDBConfig  `toml:"cassandra_config"`
	MySQLConfig     MySQLDBConfig `toml:"mysql_config"`
}

type Client struct {
	Config ClientConfig
}

var DefaultConfig = ClientConfig{
	DBType: DBTypeCass,
	CassandraConfig: CassDBConfig{
		ServerList: "localhost",
		Port:       9042,
		KeySpace:   "TestKeySpace",
	},
	MySQLConfig: MySQLDBConfig{},
}

// NewDBClient allocates and returns a new database client using the provided config.
func NewDBClient(conf ClientConfig) (DBClient, error) {
	switch conf.DBType {
	case DBTypeCass:
		return cassandradb.NewClient(conf.CassandraConfig.ServerList, conf.CassandraConfig.KeySpace)
	default:
		return nil, ops.ErrDBUnsupported
	}
}
