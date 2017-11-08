package goava

import (
	"github.com/goava/driver/cassandradb"
	"github.com/goava/ops"
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

/*
func NewConfig(config Config) (*Config, error) {
  conf := &config
  flagsInf, err := config.RegisterFlags(conf)
  if err != nil || flagsInf == nil {
    fmt.Printf("could not register commandline config flags :: %v", err)
    return nil, err
  }
  flags := flagsInf.(*Config)
  flag.Parse()

  configFile := flags.ConfigFile
  if configFile != "" {
    // config file has been specified
    if util.FileExists(configFile) {
      fmt.Printf("loading config file from %s\n", configFile)
      if _, err := toml.DecodeFile(configFile, conf); err != nil {
        fmt.Printf("error reading config from file: %s :: %v\n", configFile,
          err)
        return nil, err
      }
    } else {
      fmt.Printf("did not find config file: %s\n", configFile)
      return nil, os.ErrNotExist
    }
  }
  config.CheckFlagOverride(conf, flags)
  return conf, nil
}
*/
