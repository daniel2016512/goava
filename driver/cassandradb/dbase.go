package cassandradb

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/*
 * This library allows developers to create a Cassandra table by specifiying
 * all the properties in its corresponding struct using a "cql" tag. All the
 * parameters of a create table can be specified:
 *
 */

// Name of the struct tag used to create or query cassandra database.
const tagName = "cql"

// cql tag keys
const cnConst = "column_name"
const ctConst = "column_type"
const cstConst = "column_subtype"
const pkConst = "primary_key"
const ckConst = "clustering_key"
const ikConst = "index_key"
const obnConst = "order_by_num"
const obConst = "order_by"
const cktConst = "column_keytype"
const cvtConst = "column_valuetype"

var primarykeys []string
var clusteringkeys []string
var indexkeys []string
var orderby []string

var typeMap = map[string]string{
	"string":    "text",
	"int":       "int",
	"int8":      "smallint",
	"int16":     "smallint",
	"int32":     "int",
	"int64":     "int",
	"bool":      "boolean",
	"uuid":      "uuid",
	"time":      "time",
	"timestamp": "timestamp",
	"counter":   "counter",
	"byte":      "byte",
}

type Entity struct {
	fieldName        string
	columnName       string
	columnType       string
	columnSubType    string
	columnKeyType    string
	columnValType    string
	primaryKey       bool
	primaryKeyNum    int
	clusteringKey    bool
	clusteringKeyNum int
	orderbyField     string
	orderbyFieldNum  int
	indexKey         bool
}

// parses each entry in the struct to build the characteristic of a given field
func CreateEntity(s interface{}) ([]Entity, error) {

	v := reflect.ValueOf(s)
	counterFlag := false // by default assume no counters
	entities := []Entity{}
	for i := 0; i < v.NumField(); i++ {
		// Get the field tag value
		tag := v.Type().Field(i).Tag.Get(tagName)
		fieldType := v.Type().Field(i).Type.Name()
		fieldName := v.Type().Field(i).Name

		// Skip if tag is not defined or ignored
		if tag == "" || tag == "-" {
			continue
		}

		// fmt.Printf("field, tag : %s, %s , %s\n", v.Type().Field(i).Name, tag, fieldType)
		column := Entity{}
		column.fieldName = v.Type().Field(i).Name
		tagFields := strings.Split(tag, ",")
		m := make(map[string]string)
		count := 0
		for _, tagField := range tagFields {
			kv := strings.Split(tagField, "=")
			// fmt.Println("kv length %d %v", len(kv), kv)

			if len(kv) != 2 {
				return nil, CassandraDBError{
					time.Now(),
					"invlid format in cql tag",
					tagField,
				}
			}
			if m[kv[0]] != "" {
				return nil, CassandraDBError{
					time.Now(),
					"duplicate entry in tagField",
					tagField,
				}
			}
			m[kv[0]] = kv[1]
			count = count + 1
			// fmt.Println(count)
		}
		// fmt.Println("here1")
		val, ok := m[cnConst]
		if !ok {
			val = strings.ToLower(fieldName)
		} else {
			val = strings.ToLower(val)
		}
		column.columnName = val

		val, ok = m[ctConst]
		// fmt.Printf("RAW COLUMN TYPE FROM VAL :: %s\n", val)
		if !ok {
			val = fieldType
		}
		// fmt.Printf("COLUMN TYPE FROM VAL :: %s\n", val)
		column.columnType = val
		if column.columnType == "collection" {
			if counterFlag {
				return nil, errors.New("cannot have other column types in table with counters")
			}
			val, ok = m[cstConst]
			if !ok {
				return nil, CassandraDBError{
					time.Now(),
					"no collection subtype",
					"tagField",
				}
			}
			column.columnSubType = val
			if column.columnSubType == "map" {
				key1, ok1 := m[cktConst]
				val1, ok2 := m[cvtConst]
				if !ok1 || !ok2 {
					return nil, CassandraDBError{
						time.Now(),
						"map collection type without key and value types",
						"tagField",
					}
				}
				column.columnKeyType = key1
				keyType := typeMap[key1]
				if keyType != "" {
					column.columnKeyType = keyType
				}
				column.columnValType = val1
				valType := typeMap[val1]
				if valType != "" {
					column.columnValType = valType
				}

			} else if column.columnSubType == "list" || column.columnSubType == "set" {
				val, ok = m[cvtConst]
				if !ok {
					return nil, CassandraDBError{
						time.Now(),
						"set or list collection type without value types",
						"tagField",
					}
				}
				column.columnValType = val
				valType := typeMap[val]
				if valType != "" {
					column.columnValType = valType
				}
			}
		} else {
			newColType := typeMap[column.columnType]
			if newColType != "" {
				column.columnType = newColType
				if newColType == "counter" {
					counterFlag = true
				} else {
					if counterFlag {
						return nil, errors.New("cannot have other column types in table with counters")
					}
				}
			}
		}
		// fmt.Printf("COLUMN TYPE FROM ENTITY :: %s\n", column.columnType)
		val, ok = m[pkConst]
		if ok {
			column.primaryKey = true
			val, ok = m[pkConst]
			if !ok {
				return nil, CassandraDBError{
					time.Now(),
					"primary key must have number",
					"tagField",
				}
			}
			column.primaryKeyNum, _ = strconv.Atoi(val)
		}

		val, ok = m[ckConst]
		if ok {
			column.clusteringKey = true
			val, ok = m[ckConst]
			if !ok {
				return nil, CassandraDBError{
					time.Now(),
					"clustering key must have number",
					"tagField",
				}
			}
			column.clusteringKeyNum, _ = strconv.Atoi(val)
		}

		val, ok = m[ikConst]
		log.Printf("%s : %t\n", val, ok)
		if ok {
			column.indexKey = true
		}

		val, ok = m[obConst]
		if ok {
			val = strings.ToUpper(val)
			if val == "ASC" || val == "DESC" {
				column.orderbyField = val
			} else {
				return nil, CassandraDBError{
					time.Now(),
					"order by should be asc or desc",
					"tagField",
				}
			}
			val, ok := m[obnConst]
			if !ok {
				return nil, CassandraDBError{
					time.Now(),
					"order by field must have number",
					"tagField",
				}
			}
			column.orderbyFieldNum, _ = strconv.Atoi(val)
		}

		entities = append(entities, column)

	}
	return entities, nil
}
