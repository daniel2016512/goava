package cassandradb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/goava/ops"
	"github.com/gocql/gocql"
)

type KeySpace struct {
	sync.RWMutex
	dbSession *gocql.Session
	Name      string
	Tables    map[string]*Table
	Backups   []string
}

func GetKeySpace(name string, dbSession *gocql.Session) *KeySpace {
	ks := KeySpace{dbSession: dbSession,
		Name:   name,
		Tables: make(map[string]*Table)}

	// TODO : get list of tables in this database
	return &ks
}

// doesTableExist check to see if a given column family exists.
func (k *KeySpace) DoesTableExist(keySpace string, tableName string) (bool, error) {

	if k.dbSession == nil {
		return false, errors.New("No valid session found")
	}
	var name string
	queryString := fmt.Sprintf("SELECT table_name FROM "+
		"system_schema.tables WHERE keyspace_name = '%s' AND "+
		"table_name = '%s' ALLOW FILTERING",
		keySpace, tableName)

	err := k.dbSession.Query(queryString).Consistency(gocql.One).Scan(&name)
	if err != nil {
		if err.Error() == "not found" {
			return false, nil
		}
		log.Printf("could not read from system table: %v", err)
		return false, err
	}
	if len(name) > 0 {
		log.Printf("table exists: %s", name)
		return true, nil
	}
	return false, nil
}

func (k *KeySpace) insertTable(t *Table) {
	k.Lock()
	defer k.Unlock()
	k.Tables[t.Name] = t
}

func (k *KeySpace) removeTable(tableName string) {
	k.Lock()
	defer k.Unlock()
	delete(k.Tables, tableName)
}

// Create a cassandra database
func (k *KeySpace) CreateTable(tableName string, tableModel interface{}) (ops.Table, error) {

	if k.Name == "" {
		return nil, ops.ErrInvalidKeyspace
	}

	entities, err := CreateEntity(tableModel)
	if err != nil {
		log.Printf("Error creating table %s : %s", tableName, err)
		return nil, err
	}

	now := time.Now()
	table := &Table{Name: tableName,
		KeySpace:  k.Name,
		entities:  entities,
		createdAt: now,
		updatedAt: now,
		dbSession: k.dbSession,
		dataModel: tableModel}

	exists, _ := k.DoesTableExist(k.Name, tableName)
	if exists {
		k.insertTable(table)
		return table, ops.ErrTableExist
	}
	log.Printf("creating table: %s", tableName)

	// column := make([]string, len(entities))
	// ctype := make([]string, len(entities))
	// pks := make([]string, len(entities))
	var pks []string
	// cks := make([]string, len(entities))
	var cks []string
	// corders := make([]string, len(entities))
	var corders []string
	// iks := make([]string, len(entities))
	var iks []string

	// construct the table
	var buffer bytes.Buffer
	buffer.WriteString("create table IF NOT EXISTS ")
	buffer.WriteString(k.Name)
	buffer.WriteString(".")
	buffer.WriteString(tableName)
	buffer.WriteString(" ( ")
	for _, entity := range entities {
		if entity.columnType == "collection" {
			if entity.columnSubType == "map" {
				field := fmt.Sprintf(" %s map<%s,%s> ", entity.columnName, entity.columnKeyType, entity.columnValType)
				buffer.WriteString(field)
			} else if entity.columnSubType == "set" {
				field := fmt.Sprintf(" %s set<%s> ", entity.columnName, entity.columnValType)
				buffer.WriteString(field)
			} else if entity.columnSubType == "list" {
				field := fmt.Sprintf(" %s list<%s> ", entity.columnName, entity.columnValType)
				buffer.WriteString(field)
			} else {
				return nil, errors.New("invalid collection type : entity.columnSubType")
			}
		} else {
			// fmt.Printf(" COL KEY VAL :: %s %s \n", entity.columnName, entity.columnType)
			field := fmt.Sprintf(" %s %s ", entity.columnName, entity.columnType)
			buffer.WriteString(field)
		}
		buffer.WriteString(", ")

		if entity.primaryKey {
			// pks[entity.primaryKeyNum] = entity.columnName
			pks = append(pks, entity.columnName)
		}
		if entity.clusteringKey {
			// cks[entity.clusteringKeyNum] = entity.columnName
			cks = append(cks, entity.columnName)
		}
		if entity.orderbyField != "" {
			// corders[entity.orderbyFieldNum] = fmt.Sprintf("%s %s", entity.columnName, entity.orderbyField)
			corders = append(corders, fmt.Sprintf("%s %s", entity.columnName, entity.orderbyField))
		}

		if entity.indexKey {
			// fmt.Printf("index field :: %s\n", entity.columnName)
			// iks[index] = entity.columnName
			iks = append(iks, entity.columnName)
		}
	}
	multiplePKs := false
	pklen := len(pks)
	if pklen > 1 {
		multiplePKs = true
		buffer.WriteString(" PRIMARY KEY ((")
	} else {
		buffer.WriteString(" PRIMARY KEY (")
	}
	for idx, pk := range pks {
		if idx == 0 {
			buffer.WriteString(pk)
		} else {
			if pk == "" {
				break
			} else {
				buffer.WriteString(",")
				buffer.WriteString(pk)
			}
		}
	}
	if multiplePKs {
		buffer.WriteString(")")
	}

	cklen := len(cks)
	if cklen > 0 {
		if multiplePKs {
			buffer.WriteString(",")
		}
		for idx, ck := range cks {
			if idx == 0 {
				buffer.WriteString(ck)
			} else {
				if ck == "" {
					break
				}
				buffer.WriteString(",")
				buffer.WriteString(ck)
			}
		}
		buffer.WriteString("))")
	} else {
		buffer.WriteString(")")
	}

	orderlen := len(corders)
	if orderlen > 0 { // table has clustering orders
		buffer.WriteString(" WITH CLUSTERING ORDER BY (")
		for cidx, corder := range corders {
			if cidx == 0 {
				buffer.WriteString(corder)
			} else {
				if corder == "" {
					break
				}
				buffer.WriteString(",")
				buffer.WriteString(corder)
			}
		}
		buffer.WriteString(")")
	} else {
		buffer.WriteString(")")
	}
	buffer.WriteString(";")
	fmt.Println("CREATE TABLE: " + buffer.String() + "\n")
	queryStr := buffer.String()
	query, err := CreateQuery(k.dbSession, queryStr)
	if err != nil {
		return nil, err
	}

	tableCreateErr := ExecQuery(query)

	if tableCreateErr != nil {
		return nil, tableCreateErr
	}

	iklen := len(iks)
	log.Printf("index length :: %d\n", iklen)
	if iklen > 0 { // table has index columns
		for _, ik := range iks {
			log.Printf("index key :: %s\n", ik)
			if ik != "" {
				CreateIndex(k.dbSession, k.Name, tableName, ik)
			}
		}
	}
	k.insertTable(table)
	return table, nil
}

// Drop a cassandra database table
func (k *KeySpace) DropTable(tableName string) error {
	dropStr := fmt.Sprintf("DROP TABLE %s.%s", k.Name, tableName)
	cassQuery, err := CreateQuery(k.dbSession, dropStr)
	if err != nil {
		return err
	}
	if err = ExecQuery(cassQuery); err != nil {
		log.Printf("could not drop table: %s :: %v", tableName, err)
	}
	log.Printf("dropped table: %s", tableName)

	// remove entry from keyspace map
	k.removeTable(tableName)

	return nil

}

func (k *KeySpace) GetTable(tableName string) (ops.Table, error) {
	k.Lock()
	defer k.Unlock()
	t, ok := k.Tables[tableName]

	if ok {
		return t, nil
	}
	return nil, ops.ErrTableNA

}

func (k *KeySpace) AlterTable(tableName string) error {
	return nil
}

func (k *KeySpace) RestoreTable(tableName string) error {
	return nil
}

func (k *KeySpace) BackupDB(name string) error {
	return nil
}

func (k *KeySpace) RestoreDB(name string) error {
	return nil
}
