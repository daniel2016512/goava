package cassandradb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/goava/whc"
)

// housekeeping operations
// support for byte

type Table struct {
	sync.RWMutex
	dbSession *gocql.Session
	Name      string
	KeySpace  string
	entities  []Entity
	dataModel interface{}
	createdAt time.Time
	updatedAt time.Time
}

/*
type CQLOperatorType string

const (
	EQ          CQLOperatorType = "="
	LT          CQLOperatorType = "<"
	GT          CQLOperatorType = ">"
	LTE         CQLOperatorType = "<="
	GTE         CQLOperatorType = "=>"
	NOTEQ       CQLOperatorType = "!="
	IN          CQLOperatorType = "IN"
	CONTAINS    CQLOperatorType = "CONTAINS"
	CONTAINSKEY CQLOperatorType = "CONTAINS KEY"
)


type WhereClauseType struct {
	columnName string
	// relationType CQLOperatorType
	relationType string
	columnValue  interface{}
}
*/

// AlterTable
func (t *Table) AlterTable(data interface{}) error {

	return nil
}

// InsertRow
func (t *Table) Insert(data interface{}) error {

	var buffer bytes.Buffer
	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(t.KeySpace)
	buffer.WriteString(".")
	buffer.WriteString(t.Name)
	buffer.WriteString(" (")

	counter := len(t.entities)
	for _, entity := range t.entities {
		buffer.WriteString(entity.columnName)
		if counter > 1 {
			buffer.WriteString(", ")
			counter--
		} else {
			buffer.WriteString(") ")
		}
	}
	buffer.WriteString("VALUES (")

	var v = reflect.ValueOf(data)

	if v.Kind() == reflect.Struct {
		// fmt.Println("\n is a struct")
	}

	val := reflect.ValueOf(data).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		kindType := valueField.Kind()
		typeField := val.Type().Field(i)
		log.Printf("Type : %s , Value : %v , Kind : %s", typeField.Name, valueField.Interface(), kindType)

		if kindType == reflect.Map {
			var mapBuffer bytes.Buffer
			mapBuffer.WriteString("{")
			iface := valueField.Interface()
			a := iface.(map[string]string)

			flag := true
			// for _, key := range valueField.MapKeys() {
			for k, v := range a {
				if flag {
					flag = false
				} else {
					mapBuffer.WriteString(", ")
				}
				mapBuffer.WriteString(checkTypeAndWrite(t.entities[i].columnKeyType, k))
				mapBuffer.WriteString(" : ")
				mapBuffer.WriteString(checkTypeAndWrite(t.entities[i].columnValType, v))
			}
			mapBuffer.WriteString("}")
			buffer.WriteString(mapBuffer.String())

		} else if kindType == reflect.Slice {
			if t.entities[i].columnSubType == "set" {
				s := reflect.ValueOf(valueField.Interface())
				var setBuffer bytes.Buffer
				setBuffer.WriteString("{")
				flag := true
				for j := 0; j < s.Len(); j++ {
					if flag {
						flag = false
					} else {
						setBuffer.WriteString(", ")
					}
					setBuffer.WriteString(checkTypeAndWrite(t.entities[i].columnValType, s.Index(j)))
				}
				setBuffer.WriteString("}")
				buffer.WriteString(setBuffer.String())

			} else if t.entities[i].columnSubType == "list" {
				s := reflect.ValueOf(valueField.Interface())
				var listBuffer bytes.Buffer
				listBuffer.WriteString("[")
				flag := true
				for j := 0; j < s.Len(); j++ {
					if flag {
						flag = false
					} else {
						listBuffer.WriteString(", ")
					}
					listBuffer.WriteString(checkTypeAndWrite(t.entities[i].columnValType, s.Index(j)))
				}
				listBuffer.WriteString("]")
				buffer.WriteString(listBuffer.String())
			}

		} else {
			// buffer.WriteString(fmt.Sprintf("%v",valueField.Interface()))
			buffer.WriteString(checkTypeAndWrite(t.entities[i].columnType, valueField.Interface()))
		}

		if i < val.NumField()-1 {
			buffer.WriteString(", ")
		} else {
			buffer.WriteString(");")
		}
	}
	log.Printf("insert query : %s", buffer.String())

	if err := t.dbSession.Query(buffer.String()).Exec(); err != nil {
		return err
	}
	return nil

}

// DeleteRows deletes one or more rows from a Cassandra table
func (t *Table) Delete(deleteColumnList []string, whereClause []whc.WhereClauseType) error {

	var buffer bytes.Buffer
	buffer.WriteString("DELETE ")
	if len(deleteColumnList) > 0 {
		// fmt.Printf("delete columns: %v", deleteColumnList)
		flag := true
		for _, v := range deleteColumnList {
			if flag {
				flag = false
			} else {
				buffer.WriteString(" , ")
			}
			buffer.WriteString(v)
		}
	}

	buffer.WriteString(" FROM ")
	buffer.WriteString(t.KeySpace)
	buffer.WriteString(".")
	buffer.WriteString(t.Name)

	if len(whereClause) == 0 {
		return errors.New(fmt.Sprintf("cannot delete without where clause: %s", t.Name))
	}
	buffer.WriteString(" WHERE ")
	flag := true
	for i := 0; i < len(whereClause); i++ {
		if flag {
			flag = false
		} else {
			buffer.WriteString(" AND ")
		}
		wc := whereClause[i]
		fieldFound := false
		for _, entity := range t.entities {
			if entity.columnName == wc.ColumnName {
				wc := whereClause[i]
				buffer.WriteString(wc.ColumnName)
				buffer.WriteString(" ")
				buffer.WriteString(wc.RelationType)
				buffer.WriteString(" ")
				if wc.RelationType == "in" {
					buffer.WriteString("(")
					v := wc.ColumnValue
					switch reflect.TypeOf(v).Kind() {
					case reflect.Slice:
						s := reflect.ValueOf(v)
						if s.Len() == 0 {
							return errors.New(fmt.Sprintf("invalid where clause in delete query, no values for \"in\" operator : %s", wc.ColumnName))
						}
						commaFlag := true
						for j := 0; j < s.Len(); j++ {
							if commaFlag {
								commaFlag = false
							} else {
								buffer.WriteString(", ")
							}
							buffer.WriteString(fmt.Sprintf("%v", s.Index(j)))
						}
					default:
						return errors.New(fmt.Sprintf("invalid datatype in update whereClause , should be an array when using \"in\" : %s", wc.ColumnName))
					}
					buffer.WriteString(")")
				} else {
					buffer.WriteString(checkTypeAndWrite(entity.columnType, wc.ColumnValue))
				}
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return errors.New(fmt.Sprintf("invalid field in update where clause :: %s", wc.ColumnName))
		}
	}
	buffer.WriteString(";")
	// fmt.Printf("delete query : %s \n", buffer.String())
	if err := t.dbSession.Query(buffer.String()).Exec(); err != nil {
		return err
	}
	return nil
}

// Updates a Row where the entire updated row is supplied. This is different from
// updating a row by supplying only the fields that have changed
func (t *Table) Update(x interface{}) error {

	// return nil

	// parse thru all non primary key rows and create a map
	// create a where clause using the primary key fields and values
	// invoke the updatebyfield method

	// v := reflect.ValueOf(x)
	// fmt.Printf("\n\n********\n inside update full object :: %v\n", x)

	s := reflect.ValueOf(x)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	// typeOfS := s.Type()
	var whereClause = []whc.WhereClauseType{}
	updates := make(map[string]interface{})

	for i := 0; i < s.NumField(); i++ {
		// fieldType := s.Type().Field(i).Type.Name()
		fieldName := s.Type().Field(i).Name
		f := s.Field(i)
		// fn := typeOfS.Field(i).Name
		val := f.Interface()
		for _, entity := range t.entities {
			// fmt.Printf("column name :: %s\n", entity.columnName)
			// fmt.Printf("field name :: %s\n", entity.fieldName)
			// fmt.Printf("field name from struct :: %s\n", fieldName)

			if fieldName == entity.fieldName {
				if entity.primaryKey {
					w := whc.WhereClauseType{
						ColumnName:   entity.columnName,
						RelationType: "=",
						ColumnValue:  val,
						// ColumnValue : checkTypeAndWrite(entity.columnType, val),
					}
					whereClause = append(whereClause, w)
				} else {
					if entity.columnType == "collection" {
						var col []interface{}
						col = append(col, "all")
						// fmt.Printf("COLLECTION VAL : %v\n", val)
						col = append(col, val)
						updates[entity.columnName] = col
					} else {
						updates[entity.columnName] = val
						// updates[entity.columnName] =  checkTypeAndWrite(entity.columnType, val)
					}
				}
			}
		}
	}
	// fmt.Printf("Update map :: %v\n", updates)
	// fmt.Printf("Update where clause :: %v\n", whereClause)
	return t.UpdateFields(updates, nil, whereClause)
}

// UpdateFields updates one or more fields in a given Cassandra table row
// For updating the entire row use Update() method
func (t *Table) UpdateFields(updateMap, updateParm map[string]interface{},
	whereClause []whc.WhereClauseType) error {

	// https://gist.github.com/drewolson/4771479
	// https://play.golang.org/p/Cj9oPPGSLM

	// var updateFields map[string]interface{}
	// updateFields := make(map[string]interface{})

	// fmt.Printf("Update field and values : %v\n", updateMap)
	if updateMap == nil {
		return errors.New("Nothing to update")
	}

	var buffer bytes.Buffer
	buffer.WriteString("UPDATE ")
	buffer.WriteString(t.KeySpace)
	buffer.WriteString(".")
	buffer.WriteString(t.Name)
	buffer.WriteString(" ")

	if len(updateParm) > 0 {
		buffer.WriteString("USING ")
		flag := true
		for k, v := range updateParm {
			if flag {
				flag = false
			} else {
				buffer.WriteString(" AND ")
			}
			buffer.WriteString(k)
			buffer.WriteString(" ")
			buffer.WriteString(fmt.Sprintf("%v", v))
		}
	}

	if len(updateMap) > 0 {
		buffer.WriteString(" SET ")
		flag := true
		for k, v := range updateMap {
			// fmt.Printf("Key :: %v\n", k)
			// fmt.Printf("Val :: %v\n", v)
			if flag {
				flag = false
			} else {
				buffer.WriteString(" , ")
			}
			for _, entity := range t.entities {
				// fmt.Printf("column name :: %s\n", entity.columnName)
				// fmt.Printf("field name :: %s\n", entity.fieldName)
				if k == entity.columnName {
					// fmt.Println("Entity columntype " + entity.columnType)
					if entity.columnType == "counter" {
						switch reflect.TypeOf(v).Kind() {
						case reflect.Slice:
							s := reflect.ValueOf(v)
							if s.Len() != 2 {
								return errors.New(fmt.Sprintf("invalid update values for counter field : %s", k))
							}
							buffer.WriteString(k)
							buffer.WriteString(" = ")
							buffer.WriteString(k)
							buffer.WriteString(" ")
							buffer.WriteString(fmt.Sprintf("%v", s.Index(0)))
							buffer.WriteString(" ")
							buffer.WriteString(fmt.Sprintf("%v", s.Index(1)))
						}
					} else if entity.columnType == "collection" {
						openBracket := "{"
						closeBracket := "}"
						if entity.columnSubType == "list" {
							openBracket = "["
							closeBracket = "]"
						}
						switch reflect.TypeOf(v).Kind() {
						case reflect.Slice:
							s := reflect.ValueOf(v)
							// fmt.Printf("S KIND 1:: %v\n", s.Kind())
							/*if s.Kind() == reflect.Ptr {
							    s = s.Elem()
							    fmt.Printf("S KIND 2:: %v\n", s.Kind())
							    fmt.Printf("S value :: %v\n", s.Interface())
							}*/

							if s.Len() < 2 {
								return errors.New(fmt.Sprintf("too few values for set field : %s", k))
							}
							buffer.WriteString(k)
							buffer.WriteString(" = ")
							setterType := fmt.Sprintf("%s", s.Index(0))
							switch setterType {
							case "all":
								buffer.WriteString(openBracket)
								// fmt.Printf("COLL VALUES :: %v\n", s.Index(1))
								colStr, errBuf := getCollectionToBuffer(s.Index(1).Interface(), entity.columnName, entity.columnSubType)
								if errBuf != nil {
									return errBuf
								}
								// fmt.Printf("collection buffer :: %s\n", colStr)
								buffer.WriteString(colStr)
								buffer.WriteString(closeBracket)
							case "add":
								buffer.WriteString(k)
								buffer.WriteString(" ")
								buffer.WriteString("+ ")
								buffer.WriteString(openBracket)
								colStr, errBuf := getCollectionToBuffer(s.Index(1).Interface(), entity.columnName, entity.columnSubType)
								if errBuf != nil {
									return errBuf
								}
								// fmt.Printf("collection buffer :: %s\n", colStr)
								buffer.WriteString(colStr)
								buffer.WriteString(closeBracket)
							case "remove":
								buffer.WriteString(k)
								buffer.WriteString(" ")
								buffer.WriteString("- ")
								buffer.WriteString(openBracket)
								colStr, errBuf := getCollectionToBuffer(s.Index(1).Interface(), entity.columnName, entity.columnSubType)
								if errBuf != nil {
									return errBuf
								}
								// fmt.Printf("collection buffer :: %s\n", colStr)
								buffer.WriteString(colStr)
								buffer.WriteString(closeBracket)
							}
						default:
							return errors.New(fmt.Sprintf("invalid update values for set or list field : %s", k))
						}
					} else { // regular column types
						buffer.WriteString(k)
						buffer.WriteString(" = ")
						// buffer.WriteString(fmt.Sprintf("%v", v))
						buffer.WriteString(checkTypeAndWrite(entity.columnType, v))
					}
					break
				}
			}

		}
	}

	buffer.WriteString(" WHERE")
	buffer.WriteString(" ")
	if len(whereClause) == 0 {
		return errors.New("no where clause in update statement")
	}
	flag := true
	for i := 0; i < len(whereClause); i++ {
		if flag {
			flag = false
		} else {
			buffer.WriteString(" AND ")
		}
		wc := whereClause[i]
		fieldFound := false
		for _, entity := range t.entities {
			if entity.columnName == wc.ColumnName {
				wc := whereClause[i]
				buffer.WriteString(wc.ColumnName)
				buffer.WriteString(" ")
				buffer.WriteString(wc.RelationType)
				buffer.WriteString(" ")
				buffer.WriteString(checkTypeAndWrite(entity.columnType, wc.ColumnValue))
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return errors.New(fmt.Sprintf("invalid field in update where clause :: %s", wc.ColumnName))
		}
	}
	buffer.WriteString(";")
	// fmt.Printf("update query : %s \n", buffer.String())
	if err := t.dbSession.Query(buffer.String()).Exec(); err != nil {
		return err
	}
	return nil
}

/*
func (t *Table) ReadAndBind(x interface{}, whereClause []whc.WhereClauseType,
	groupByClause []string, orderByClause map[string]string) error {

	buffer, _ := getReadQueryString(t.entities, t.KeySpace, t.Name, whereClause,
		groupByClause, orderByClause)
	log.Printf("select one query : %s", buffer.String())

	resultMap := make(map[string]interface{})
	if err := t.dbSession.Query(buffer.String()).MapScan(resultMap); err != nil {
		return err
	}

	xv := reflect.ValueOf(&x).Elem() 
	xt := xv.Type()
	var args []interface{}

	for i := 0; i < xt.NumField(); i++ {
		addr := xv.Field(i).Addr().Interface()
		args = append(args, addr)
	}

	if iter.Scan(args...) {
	} else {
		iter.Close()
	}
	fmt.Println("Data ", utd)
	
	for k, v := range resultMap {
		// fmt.Printf("Map Key :: %s\n", k)
		for _, entity := range t.entities {
			if entity.columnName == k {
				err := setField(x, entity.fieldName, v)
				if err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}
*/

// Reads a single row and binds  result to the supplied structure
func (t *Table) ReadAndBind(x interface{}, whereClause []whc.WhereClauseType,
	groupByClause []string, orderByClause map[string]string) error {

	buffer, _ := getReadQueryString(t.entities, t.KeySpace, t.Name, whereClause,
		groupByClause, orderByClause)
	log.Printf("select one query : %s", buffer.String())

	// resultMap := make(map[string]interface{})
	xv := reflect.ValueOf(x).Elem() 
	xt := xv.Type()
	var args []interface{}
	for i := 0; i < xt.NumField(); i++ {
		addr := xv.Field(i).Addr().Interface()
		args = append(args, addr)
	}
	if err := t.dbSession.Query(buffer.String()).Scan(args...); err != nil {
		return err
	}
	return nil
}

/*
func (t *Table) Read(whereClause []whc.WhereClauseType, groupByClause []string,
	orderByClause map[string]string) (interface{}, error) {

	buffer, _ := getReadQueryString(t.entities, t.KeySpace, t.Name, whereClause,
		groupByClause, orderByClause)
	log.Printf("select one query : %s", buffer.String())

	resultMap := make(map[string]interface{})
	if err := t.dbSession.Query(buffer.String()).MapScan(resultMap); err != nil {
		return nil, err
	}
	// fmt.Printf("MapScan result : %v\n", resultMap)
	typ := reflect.TypeOf(t.dataModel)
	one := reflect.New(typ)
	oneVal := one.Elem()
	for k, v := range resultMap {
		// fmt.Printf("Key from returned MAP :: %s\n", k)
		for _, entity := range t.entities {

			if entity.columnName == k {

				structFieldValue := oneVal.FieldByName(entity.fieldName)
				if !structFieldValue.IsValid() {
					log.Printf("Field not found, skipping: %s", entity.fieldName)
					break
				}
				// fmt.Printf("Field value :: %v\n", structFieldValue)
				if !structFieldValue.CanSet() {
					log.Printf("Cannot set %s field value", entity.fieldName)
					break
				}
				structFieldType := structFieldValue.Type()
				val := reflect.ValueOf(v)
				if structFieldType != val.Type() {
					// fmt.Println("%v : %v", structFieldType, val.Type())
					log.Printf("Field value type not matching field type")
					break
				}
				structFieldValue.Set(val)
				// fmt.Printf("value has been set :: %v\n", structFieldValue)
				break
			}
		}
	}

	// fmt.Printf("query result in var : %v\n", oneVal)
	return oneVal.Interface(), nil
}
*/

// Read one row from table. Only the first row is returned
func (t *Table) Read(whereClause []whc.WhereClauseType, groupByClause []string,
	orderByClause map[string]string) (interface{}, error) {

	buffer, _ := getReadQueryString(t.entities, t.KeySpace, t.Name, whereClause,
		groupByClause, orderByClause)
	log.Printf("select one query : %s", buffer.String())

	resultMap := make(map[string]interface{})
	if err := t.dbSession.Query(buffer.String()).MapScan(resultMap); err != nil {
		return nil, err
	}

	xt := reflect.TypeOf(t.dataModel)
	xv := reflect.New(xt)
	s := xv.Elem()
	var args []interface{}
	for i := 0; i < xt.NumField(); i++ {
		addr := s.Field(i).Addr().Interface()
		args = append(args, addr)
	}
	if err := t.dbSession.Query(buffer.String()).Scan(args...); err != nil {
		return nil, err
	}
	fmt.Printf("query result in var : %v\n", xv)
	return s.Interface(), nil
}

// Lists multiple rows from table. This call supports pagination
func (t *Table) List(whereClause []whc.WhereClauseType,
	groupByClause []string, orderByClause map[string]string,
	count int, pageIndex string) (interface{}, error) {

	buffer, _ := getReadQueryString(t.entities, t.KeySpace, t.Name, whereClause,
		groupByClause, orderByClause)
	// fmt.Printf("select multiple query : %s\n", buffer.String())

	iter := t.dbSession.Query(buffer.String()).Consistency(gocql.One).Iter()
	resultMap := make(map[string]interface{})

	many := reflect.New(reflect.SliceOf(reflect.TypeOf(t.dataModel)))
	manyVals := many.Elem()

	for iter.MapScan(resultMap) {
		// fmt.Printf("iter result : %v\n", resultMap)
		typ := reflect.TypeOf(t.dataModel)
		one := reflect.New(typ)
		oneVal := one.Elem()
		for k, v := range resultMap {
			for _, entity := range t.entities {
				if entity.columnName == k {

					// check before calling function
					// fmt.Printf("Field Name :: %s\n", entity.fieldName)
					structFieldValue := oneVal.FieldByName(entity.fieldName)
					if !structFieldValue.IsValid() {
						log.Printf("Field not found, skipping: %s", entity.fieldName)
						break
					}
					// fmt.Printf("Field value :: %v\n", structFieldValue)
					if !structFieldValue.CanSet() {
						log.Printf("Cannot set %s field value", entity.fieldName)
						break
					}
					structFieldType := structFieldValue.Type()
					val := reflect.ValueOf(v)
					if structFieldType != val.Type() {
						// fmt.Println("%v : %v", structFieldType, val.Type())
						log.Printf("Field value type not matching field type")
						break
					}
					structFieldValue.Set(val)
					// fmt.Printf("value has been set :: %v\n", structFieldValue)
					break
				}
			}
		}
		// fmt.Println("before adding to slice...")
		manyVals.Set(reflect.Append(manyVals, oneVal))
		// fmt.Println("after adding to slice...")
		resultMap = make(map[string]interface{})
	}
	if err := iter.Close(); err != nil {
		// fmt.Printf("err in iter query : %v", err)
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	// fmt.Printf("Multi result %v\n", manyVals)
	return manyVals.Interface(), nil
}

func (t *Table) Backup(tableName string) error {
	return nil
}

func (t *Table) Restore(tableName string) error {
	return nil
}

func getReadQueryString(entities []Entity, keySpace string, name string,
	whereClause []whc.WhereClauseType, groupByClause []string,
	orderByClause map[string]string) (bytes.Buffer, error) {

	var buffer bytes.Buffer
	buffer.WriteString("SELECT ")
	counter := len(entities)
	for _, entity := range entities {
		buffer.WriteString(entity.columnName)
		if counter > 1 {
			buffer.WriteString(", ")
			counter--
		} else {
			buffer.WriteString(" ")
		}
	}
	buffer.WriteString("FROM ")
	buffer.WriteString(keySpace)
	buffer.WriteString(".")
	buffer.WriteString(name)
	buffer.WriteString(" ")

	if whereClause != nil {
		wclen := len(whereClause)
		if wclen >= 1 {
			buffer.WriteString("WHERE ")
			flag := true
			for i := 0; i < wclen; i++ {
				if flag {
					flag = false
				} else {
					buffer.WriteString(" AND ")
				}
				for _, entity := range entities {
					wc := whereClause[i]
					if entity.columnName == wc.ColumnName {
						// fmt.Printf("Update column name type : %s, %v \n", wc.ColumnName, entity.columnType)
						buffer.WriteString(wc.ColumnName)
						buffer.WriteString(" ")
						buffer.WriteString(wc.RelationType)
						buffer.WriteString(" ")
						buffer.WriteString(checkTypeAndWrite(entity.columnType, wc.ColumnValue))
						break
					}
				}
			}
		}
	}

	if groupByClause != nil {
		gblen := len(groupByClause)
		if gblen >= 1 {
			buffer.WriteString(" GROUP BY ")
			flag := true
			for i := 0; i < gblen; i++ {
				if flag {
					flag = false
				} else {
					buffer.WriteString(" , ")
				}
				buffer.WriteString(groupByClause[i])
			}
		}
	}

	if orderByClause != nil {
		oblen := len(orderByClause)
		if oblen >= 1 {
			flag := true
			buffer.WriteString(" ORDER BY ")
			for k, v := range orderByClause {
				if flag {
					flag = false
				} else {
					buffer.WriteString(" , ")
				}
				buffer.WriteString(k)
				buffer.WriteString(" ")
				buffer.WriteString(v)
			}
		}
	}
	buffer.WriteString(";")
	return buffer, nil
}

func checkTypeAndWrite(keyType string, data interface{}) string {
	var buf bytes.Buffer
	if keyType == "string" || keyType == "text" || keyType == "ascii" || keyType == "varchar" || keyType == "inet" {
		buf.WriteString("'")
		buf.WriteString(fmt.Sprintf("%s", data))
		buf.WriteString("'")
	} else if keyType == "timestamp" {
		t, ok := data.(time.Time)
		if !ok {
			return "bad time"
		}
		buf.WriteString("'")
		buf.WriteString(fmt.Sprintf("%s", t.Format(time.RFC3339)))
		buf.WriteString("'")
	} else {
		buf.WriteString(fmt.Sprintf("%v", data))
	}
	return buf.String()
}

func getCollectionToBuffer(x interface{}, columnName string, collectionType string) (string, error) {

	var buf bytes.Buffer

	if collectionType == "list" || collectionType == "set" {
		slice, ok := x.([]string)
		if !ok {
			return "", errors.New(fmt.Sprintf("Invalid list or set data format for create or update for column \"%s\"\n", columnName))
		}
		first := true
		for _, v := range slice {
			if first {
				first = false
			} else {
				buf.WriteString(",")
			}
			buf.WriteString("'")
			buf.WriteString(fmt.Sprintf("%s", v))
			buf.WriteString("'")
		}
	} else {
		mp, ok := x.(map[string]string)
		if !ok {
			return "", errors.New(fmt.Sprintf("Invalid map data format for create or update for column \"%s\"\n", columnName))
		} else {
			first := true
			for k, v := range mp {
				if first {
					first = false
				} else {
					buf.WriteString(",")
				}
				buf.WriteString("'")
				buf.WriteString(fmt.Sprintf("%s", k))
				buf.WriteString("' : ")
				buf.WriteString("'")
				buf.WriteString(fmt.Sprintf("%s", v))
				buf.WriteString("'")
			}
		}
	}
	return buf.String(), nil
}

func fillStruct(ptr interface{}, m map[string]interface{}, entities []Entity) error {

	t := reflect.TypeOf(ptr)
	// fmt.Printf("Result OBJECT TYPE :: %s\n", t.Kind().String())

	if t.Kind() == reflect.Ptr {
		// fmt.Println("control should not come here...1")
		t = t.Elem()
	}

	sl := reflect.ValueOf(ptr)
	if t.Kind() == reflect.Ptr {
		// fmt.Println("control should not come here...2")
		sl = sl.Elem()
	}

	st := sl.Type()
	tt := reflect.TypeOf(sl)
	// fmt.Printf("****** %v\n", tt.Kind())
	if tt.Kind() == reflect.Slice {
		// fmt.Println("It is a slice type.....")
	}
	// fmt.Printf("Slice Type %s:\n", st)

	sliceType := st.Elem()
	// fmt.Printf("Slice Elem Type 1 %v:\n", sliceType)
	if sliceType.Kind() == reflect.Ptr {
		sliceType = sliceType.Elem()
	}
	// fmt.Printf("Slice Elem Type 2 %v:\n", sliceType)
	// fmt.Printf("Type of struct %v:\n", reflect.TypeOf(sliceType))

	for k, v := range m {
		// fmt.Printf("Map Key :: %s\n", k)
		for _, entity := range entities {
			if entity.columnName == k {
				err := setField(sliceType, entity.fieldName, v)
				if err != nil {
					// fmt.Printf("Set field error :: %v\n", err)
					return err
				}
				break
			}
		}

	}
	// fmt.Printf("single row 1 : %v\n", ptr)

	return nil
}

func setField(obj interface{}, name string, value interface{}) error {

	s := reflect.ValueOf(obj)
	// fmt.Printf("OBJECT TYPE :: %s\n", s.Kind().String())

	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}

	// fmt.Printf("New object type :: %s\n", s.Kind().String())
	// fmt.Printf("Number of fields in struct : %d\n", s.NumField())
	/*
		for i := 0; i < s.NumField(); i++ {
			fmt.Println(s.Type().Field(i).Name)
		}
	*/
	if s.Kind() != reflect.Struct {
		return errors.New("invalid type for query result, struct required")
	}

	structFieldValue := s.FieldByName(name)
	// fmt.Printf("Field value :: %v\n", structFieldValue)
	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		// fmt.Println("%v : %v", structFieldType, val.Type())
		return errors.New("Provided value type didn't match obj field type")
	}
	structFieldValue.Set(val)
	return nil
}
