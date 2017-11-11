package cassandradb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/goava/whc"
)

type User struct {
	Id        int               `cql:"column_name=id,primary_key=0"`
	City      string            `cql:"column_name=city,primary_key=1"`
	Email     string            `cql:"column_name=email,clustering_key=0,order_by_num=0,order_by=asc"`
	FirstName string            `cql:"column_name=firstname,clustering_key=1,order_by_num=1,order_by=asc"`
	LastName  string            `cql:"column_name=lastname,clustering_key=2,order_by_num=2,order_by=desc"`
	NickName  string            `cql:"column_name=nickname,index_key=true"`
	Zip       string            `cql:"column_name=zip,column_type=text"`
	Status    string            `cql:"column_name=status,column_type=text"`
	Friends   []string          `cql:"column_name=friends,column_type=collection,column_subtype=set,column_valuetype=string"`
	Family    []string          `cql:"column_name=family,column_type=collection,column_subtype=set,column_valuetype=string"`
	SSOIds    map[string]string `cql:"column_name=ssoids,column_type=collection,column_subtype=map,column_keytype=string,column_valuetype=string"`
}

type Provider struct {
	Id        int               `cql:"column_name=id,index_key=true"`
	State     string            `cql:"column_name=state,primary_key=0"`
	City      string            `cql:"column_name=city"`
	Email     string            `cql:"column_name=email,column_type=text"`
	FirstName string            `cql:"column_name=firstname,column_type=text"`
	LastName  string            `cql:"column_name=lastname,column_type=text"`
	NickName  string            `cql:"column_name=nickname,index_key=true"`
	Zip       string            `cql:"column_name=zip,column_type=text"`
	Status    string            `cql:"column_name=status,column_type=text"`
	Friends   []string          `cql:"column_name=friends,column_type=collection,column_subtype=set,column_valuetype=string"`
	Family    []string          `cql:"column_name=family,column_type=collection,column_subtype=list,column_valuetype=string"`
	SSOIds    map[string]string `cql:"column_name=ssoids,column_type=collection,column_subtype=map,column_keytype=string,column_valuetype=string"`
	CreatedAt time.Time         `cql:"column_name=createdAt,column_type=timestamp"`
}

type UserData struct {
        Id string `cql:"column_name=id,column_type=text,primary_key=0"`
        Uname string `cql:"column_name=uname,column_type=text"`
        Utype          string   `cql:"column_name=utype,column_type=text"`
        AccountId      string   `cql:"column_name=accountId,column_type=text"`
        Email          string   `cql:"column_name=email,column_type=text"`
        Phone          string   `cql:"column_name=phone,column_type=text,index_key=true"`
        FirstName      string   `cql:"column_name=firstName,column_type=text"`
        LastName       string   `cql:"column_name=lastName,column_type=text"`
        LookupKey      string   `cql:"column_name=lookupKey,column_type=text"`
        Password       string   `cql:"column_name=password,column_type=text"`
        PwdHash        string   `cql:"column_name=pwdHash,column_type=text"`
        RegistrationId string   `cql:"column_name=registrationId,column_type=text"`
        Status         int      `cql:"column_name=status,column_type=int"`
        GroupIDs       []string `cql:"column_name=members,column_type=collection,column_subtype=set,column_valuetype=string"`
        Street1 string `cql:"column_name=street1,column_type=text"`
        Street2 string `cql:"column_name=street2,column_type=text"`
        City    string `cql:"column_name=city,column_type=text"`
        State   string `cql:"column_name=state,column_type=text"`
        Country string `cql:"column_name=country,column_type=text"`
        ZipCode string `cql:"column_name=zipcode,column_type=text"`
        CreatedAt      time.Time `cql:"column_name=createdAt,column_type=timestamp"`
        UpdatedAt      time.Time `cql:"column_name=updatedAt,column_type=timestamp"`
        ActivationCode int       `cql:"column_name=activationCode,column_type=int"`
}

var (
	serverlist   = "127.0.0.1"
	keyspacename = "newkeyspace"
)

var Cities = []string{"London", "San Francisco", "New York", "Bangalore"}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

var numberRunes = []rune("1234567890")

func getRandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func getRandomNumber(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(numberRunes))]
	}
	return string(b)
}

func TestKeySpaceCreate(t *testing.T) {

	dbclient, errDB := NewClient(serverlist, keyspacename)
	assert.Nil(t, errDB)
	defer dbclient.Disconnect()

	// initialize the cassandra database
	dbExists, errKS1 := dbclient.DoesDBExist(keyspacename)
	assert.Nil(t, errKS1)
	assert.True(t, !dbExists, "keyspace should not exist")
	errDB2 := dbclient.CreateDB(keyspacename)
	assert.Nil(t, errDB2)
}

func TestKeySpaceList(t *testing.T) {

	dbclient, errDB := NewClient(serverlist, keyspacename)
	assert.Nil(t, errDB)
	defer dbclient.Disconnect()
	ks, errKS1 := dbclient.ListDBs()
	assert.Nil(t, errKS1)
	assert.NotNil(t, ks)
	fmt.Printf("Keyspaces : %v\n\n", ks)
}

/*func TestKeySpaceDelete(t *testing.T) {

  dbclient, errDB := NewClient(serverlist, keyspacename)
  assert.Nil(t, errDB)
    defer dbclient.Disconnect()

    // initialize the cassandra database
    dbExists, errKS1 := dbclient.DoesKeyspaceExist(keyspacename)
    assert.Nil(t, errKS1)
    assert.True(t, dbExists, "keyspace should exist")
    errDB2 := dbclient.DropKeySpace(keyspacename)
    assert.Nil(t, errDB2)
}*/

func TestCreateTable(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	assert.True(t, true, "True is true!")
	ids := map[string]string{
		"fb":   "3711",
		"goog": "2138",
		"twt":  "1908",
		"lknd": "xx912",
	}
	frnds := []string{
		"Tim",
		"Jim",
		"Schmoe",
	}
	user1 := User{
		Id:        1,
		City:      "London",
		Email:     "johndoe@london.com",
		FirstName: "John",
		LastName:  "Doe",
		NickName:  "jdoe",
		Zip:       "94567",
		Status:    "Created",
		Friends:   frnds,
		Family:    []string{"Peter", "Cramer", "Robert"},
		SSOIds:    ids,
	}

	user2 := User{
		Id:        2,
		City:      "Sydney",
		Email:     "johndoe@sydney.com",
		FirstName: "Sydney",
		LastName:  "Poiter",
		NickName:  "sidney",
		Zip:       "786876",
		Status:    "Created",
		Friends:   frnds,
		Family:    []string{"Peter", "Cramer", "Robert"},
		SSOIds:    ids,
	}

	dbclient, errDB := NewClient(serverlist, keyspacename)
	assert.Nil(t, errDB)
	defer dbclient.Disconnect()

	// table, errors := dbclient.CreateTable(keyspacename, "user", User{})
	db, errDB := dbclient.GetDB()
	assert.Nil(t, errDB)

	table, errors := db.CreateTable("user", User{})
	assert.Nil(t, errors)
	fmt.Printf("table : %+v\n", table)

	count := 1
	for count < 100 {
		user := User{
			Id:        count,
			City:      Cities[count%4],
			Email:     "User-" + strconv.Itoa(count) + "@mycompany.com",
			FirstName: getRandomString(8),
			LastName:  getRandomString(8),
			NickName:  getRandomString(7),
			Zip:       getRandomNumber(6),
			Status:    "Created",
			Friends:   []string{getRandomString(6), getRandomString(6), getRandomString(6)},
			Family:    []string{getRandomString(6), getRandomString(6), getRandomString(6)},
			SSOIds:    ids,
		}
		errInsert := table.Insert(&user)
		assert.Nil(t, errInsert)
		count++
	}

	errInsert := table.Insert(&user1)
	assert.Nil(t, errInsert)

	errInsert = table.Insert(&user2)
	assert.Nil(t, errInsert)

	// singleUser, errsr := table.Read(nil, nil, nil, User{})
	singleUser, errsr := table.Read(nil, nil, nil)
	fmt.Printf("Single User : %v\n", singleUser)
	assert.Nil(t, errsr)

	var whereClause = []whc.WhereClauseType{
		whc.WhereClauseType{
			ColumnName:   "id",
			RelationType: "=",
			ColumnValue:  1,
		},
		whc.WhereClauseType{
			ColumnName:   "city",
			RelationType: "=",
			ColumnValue:  "London",
		},
	}

	/*var orderByClause = []OrderByClauseType {
	  OrderByClauseType{
	    columnName:   "lastname",
	    orderType: "ASC",
	  },
	  OrderByClauseType{
	    columnName:   "firstname",
	    orderType: "DESC",
	  },
	}*/

	userwithWhereClause, errReadWhereClause := table.Read(whereClause, nil, nil)
	assert.Nil(t, errReadWhereClause)
	fmt.Printf("Single User with where clasue : %v\n", userwithWhereClause)

	users, errMultipleRead := table.List(nil, nil, nil, -1, "test")
	fmt.Printf("Many Users : %v\n", users)
	assert.Nil(t, errMultipleRead)

	var updateWhereClause = []whc.WhereClauseType{
		whc.WhereClauseType{
			ColumnName:   "id",
			RelationType: "=",
			ColumnValue:  1,
		},
		whc.WhereClauseType{
			ColumnName:   "city",
			RelationType: "=",
			ColumnValue:  "London",
		},
		whc.WhereClauseType{
			ColumnName:   "firstname",
			RelationType: "=",
			ColumnValue:  "John",
		},
		whc.WhereClauseType{
			ColumnName:   "lastname",
			RelationType: "=",
			ColumnValue:  "Doe",
		},
		whc.WhereClauseType{
			ColumnName:   "email",
			RelationType: "=",
			ColumnValue:  "johndoe@london.com",
		},
	}

	updates := make(map[string]interface{})
	updates["status"] = "active"
	updates["zip"] = "00011"
	updaterr := table.UpdateFields(updates, nil, updateWhereClause)
	assert.Nil(t, updaterr)

	// delete the table
	deleteErr := table.Delete(nil, updateWhereClause)
	assert.Nil(t, deleteErr)

	// New Table

	// Create Table
	table2, error2 := db.CreateTable("provider", Provider{})
	assert.Nil(t, error2)
	fmt.Printf("table : %+v\n", table2)

	now := time.Now()

	states := []string{"EX", "CA", "NY", "KA"}
	count = 1
	for count < 100 {
		user := Provider{
			Id:        count,
			City:      Cities[count%4],
			State:     states[count%4],
			Email:     "User-" + strconv.Itoa(count) + "@mycompany.com",
			FirstName: getRandomString(8),
			LastName:  getRandomString(8),
			NickName:  getRandomString(7),
			Zip:       getRandomNumber(6),
			Status:    "Created",
			Friends:   []string{getRandomString(6), getRandomString(6), getRandomString(6)},
			Family:    []string{getRandomString(6), getRandomString(6), getRandomString(6)},
			SSOIds:    ids,
			CreatedAt: now,
		}
		errInsert := table2.Insert(&user)
		assert.Nil(t, errInsert)
		count++
	}

	p := Provider{
		Id:        count,
		City:      "San Francisco",
		State:     "CA",
		Email:     "testuser",
		FirstName: "Userfirstname",
		LastName:  "Userlastname",
		NickName:  "satan",
		Zip:       "03565",
		Status:    "Created",
		Friends:   []string{getRandomString(6), getRandomString(6), getRandomString(6)},
		Family:    []string{getRandomString(6), getRandomString(6), getRandomString(6)},
		SSOIds:    ids,
		CreatedAt: now,
	}
	errInsertP := table2.Insert(&p)
	assert.Nil(t, errInsertP)

	var whereClause2 = []whc.WhereClauseType{
		whc.WhereClauseType{
			ColumnName:   "state",
			RelationType: "=",
			ColumnValue:  "EX",
		},
	}

	// orderByClause2 := make(map[string]string)
	// orderByClause2["city"] = "DESC"

	// List rows that match whereclause

	providers, errmr2 := table2.List(whereClause2, nil, nil, -1, "test")
	fmt.Printf("Many Providers : %v\n", providers)
	assert.Nil(t, errmr2)

	// Read a single row
	var whereClause3 = []whc.WhereClauseType{
		whc.WhereClauseType{
			ColumnName:   "state",
			RelationType: "=",
			ColumnValue:  "CA",
		},
	}
	p1, errsr := table2.Read(whereClause3, nil, nil)
	fmt.Printf("Single Provider : %v\n", p1)
	assert.Nil(t, errsr)

	// update the read row by passing the entire row with updated fields

	p2, ok := p1.(Provider)
	assert.False(t, !ok)
	p2.Status = "testnewupdate"
	p2.Zip = "99989"
	fmt.Printf("USER BEFORE FULL UPDATE : %v\n", p2)
	updateErr2 := table2.Update(p2)
	assert.Nil(t, updateErr2)

	// read updated row and make sure changes have happened
	p3, errp3 := table2.Read(whereClause3, nil, nil)
	assert.Nil(t, errp3)
	p4, ok2 := p3.(Provider)
	fmt.Printf("USER AFTER FULL UPDATE : %v\n", p4)
	assert.False(t, !ok2)
	assert.Equal(t, p4.Status, "testnewupdate", "user status should be equal")

	p5 := &Provider{}
	err5 := table2.ReadAndBind(p5, whereClause3, nil, nil)
	assert.Nil(t, err5)
	fmt.Printf("PRINT USER FIELD WITHOUT CASTING : %s\n", p5.Zip)

	// Create Table
	table3, error2 := db.CreateTable("userdata", UserData{})
	assert.Nil(t, error2)
	fmt.Printf("table : %+v\n", table2)

	now = time.Now()

	pvdr := UserData{
		Id:        "100",
		Uname :    "deepbhattacharjee",
		Utype :    " ",
		AccountId : "0f946e3b-a459-477b-a4c6-24d777f418d1",
		Email:  "vijay@tapaskar.com",
		Phone: "6667778888",
		FirstName: "Vijay",
		LastName:  "Tapaskar",
		LookupKey : "qnet1",
		Password : "1111111",
		PwdHash : "$2a$10$EloOw1Ee2IPQ6K/BDFCWk.Fmo1eOsICZpXonVJ8jmAtV3J9mEAvmC",
		RegistrationId : "",
		Status:    124,
		GroupIDs : []string{},
		Street1 : "Rose vue",
		Street2 : "",
		City:      "Paris",
		State:     "Paris",
		Country:   "France",
		ZipCode:    "94306",
		CreatedAt: now,
		UpdatedAt: now,
		ActivationCode : 123,
	}
	errInsert3 := table3.Insert(&pvdr)
	assert.Nil(t, errInsert3)

	// tests remaining
	// group by and orderby clause
	// pagination

}

func TestDropTable(t *testing.T) {

	dbclient, errDB := NewClient(serverlist, keyspacename)
	assert.Nil(t, errDB)
	defer dbclient.Disconnect()
	// err := dbclient.DropTable("user")
	// assert.Nil(t, err)
}
