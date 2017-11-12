package ops

import (
	"github.com/meooio/goava/whc"
)

// An interface to be implemented by all database connectors
// The interface has common methods that represent actions common to all database technologies SQL or NOSQL
// Initially we will have a cassandra implementation of this interface

const (
	SIMPLE  = "simpletable"
	KMAP    = "kmaptable"
	MAP     = "maptable"
	MMAP    = "multimaptable"
	TSERIES = "timeseriestable"
)

const (
	ASC  = "ASC"
	DESC = "DESC"
)

// Table, interface
//     Every driver needs to be support these interfaces, some databases may not implement all the functions
//     functions that are implemented, should return ENoSupport
//
//
type Table interface {
	Insert(data interface{}) error
	Delete(deleteColumnList []string, whereClause []whc.WhereClauseType) error
	// ReadByPrimaryKey(interface{}) error
	ReadAndBind(x interface{}, hereClause []whc.WhereClauseType, groupByClause []string, orderByClause map[string]string) error 
	Read(whereClause []whc.WhereClauseType, groupByClause []string, orderByClause map[string]string) (interface{}, error)
	List(whereClause []whc.WhereClauseType, groupByClause []string, orderByClause map[string]string,
		count int, pageIndex string) (interface{}, error)
	Update(data interface{}) error
	UpdateFields(updateMap, updateParm map[string]interface{}, whereClause []whc.WhereClauseType) error
	Backup(tableName string) error
	Restore(tableName string) error
	// getNext()
	// getPrev()
	// archiveRow
	// getRevisions(lastN int)

}

type Database interface {
	DoesTableExist(keySpace string, tableName string) (bool, error)
	BackupDB(name string) error
	RestoreDB(name string) error
	CreateTable(tableName string, tableModel interface{}) (Table, error)
	DropTable(tableName string) error
	AlterTable(tableName string) error
	GetTable(tableName string) (Table, error)
}
