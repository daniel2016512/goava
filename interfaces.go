package goava

import (
	"github.com/goava/ops"
)

type DBClient interface {
	Connect() error
	Disconnect() error
	Isconnected() bool
	ReConnect() error
	GetDB() (ops.Database, error)
	CreateDB(name string) error
	SetDBName(name string) error
	DropDB(name string) error
	ListDBs() ([]string, error)
	DoesDBExist(name string) (bool, error)
}
