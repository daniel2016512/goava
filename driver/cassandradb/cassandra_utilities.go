package cassandradb

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

type CassandraDBError struct {
	timeStamp time.Time
	summary   string
	detail    string
}

func (c CassandraDBError) Error() string {
	return fmt.Sprintf("%v : %s - %s", c.timeStamp, c.summary, c.detail)
}

// CassandraQuery is the type passed back after serializing the info.
type CassandraQuery struct {
	Stmt   *string
	Values []interface{}
}

func CreateIndex(dbSession *gocql.Session, keyspaceName, tableName, index string) error {
	idx := tableName + index + "_index"
	queryStr := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)", idx, keyspaceName, tableName, index)
	log.Printf("query :: %s\n", queryStr)
	cassQuery, err := CreateQuery(dbSession, queryStr)
	if err != nil {
		return err
	}
	if err = ExecQuery(cassQuery); err != nil {
		log.Printf("Create index query failed: %s :: %v", queryStr, err)
	}
	log.Printf("created index: %s", index)
	return nil
}

// CreateQuery is a helper function that constructs a gocql.Query object from
// the input query string and values varargs.
func CreateQuery(dbSession *gocql.Session, stmt string, values ...interface{}) (*gocql.Query, error) {

	return dbSession.Query(stmt, values), nil
}

// ScanQuery executes the given query on the Cassandra DB, copies the columns of
// the first selected row into the values pointed at by result and discards the
// rest. If no rows were selected, ErrNotFound is returned.
func ScanQuery(dbSession *gocql.Session, cassQuery *gocql.Query,
	results ...interface{}) error {

	if dbSession == nil {
		return errors.New("invalid DB connection")
	}
	if err := (*cassQuery).Scan(results); err != nil {
		log.Printf("error executing query: ", err)
		return err
	}
	return nil
}

// ExecQuery executes the given query on the Cassandra DB without returning any
// rows. It is a wrapper around gocql.Query.Exec() and is used by SetDB().
func ExecQuery(cassQuery *gocql.Query) error {
	if err := (*cassQuery).Exec(); err != nil {
		log.Printf("error executing query: %v :: %v", *cassQuery, err)
		return err
	}
	return nil
}
