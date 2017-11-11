// Do not need to expose any cassandra function
// Better to expose data base CRUD operations

package cassandradb

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goava/ops"
)

// Client implements the client interface to Cassandra.
// serverList is the cassandra server cluster information.
// keyspace is the keyspace to be used for this session.
type Client struct {
	sync.RWMutex
	dbSession    *gocql.Session
	serverList   string
	keyspaceName string
	keyspace     *KeySpace
	clusterCfg   *gocql.ClusterConfig
	// stats
	reconnectCtr int64
}

// NewClient returns an instance of Client after connecting
// to the cassandra server and initializing it.
// serverList is the list of cassandra servers.
// keyspace is the Cassandra keyspace used by this session.
func NewClient(serverList string, keyspace string) (*Client, error) {
	client := Client{serverList: serverList}
	if keyspace != "" {
		client.keyspaceName = keyspace
	}

	if err := client.Connect(); err != nil {
		log.Printf("error connecting to Cassandra")
		// return the initialized object rather than nil and let caller take care of reconnecting again
		return &client, err
	}
	/*if keyspace != "" {
		client.keyspace = GetKeySpace(c.keyspaceName, c.dbSession)
	} */
	return &client, nil
}

// Connect connects or reconnects to Cassandra cluster using the info supplied
// in NewClient.
func (c *Client) Connect() error {
	if c == nil {
		return fmt.Errorf("nil cassdb client context")
	}
	c.clusterCfg = gocql.NewCluster(c.serverList)
	c.clusterCfg.Consistency = gocql.Quorum
	c.clusterCfg.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 4}
	c.clusterCfg.Consistency = gocql.One
	var duration_Milliseconds time.Duration = 900 * time.Millisecond
	c.clusterCfg.Timeout = duration_Milliseconds

	if c.keyspace != nil {
		c.clusterCfg.Keyspace = c.keyspaceName
	}
	var errS error
	c.dbSession, errS = c.clusterCfg.CreateSession()
	if errS != nil {
		log.Printf("error creating Cassandra session to cluster: %v :: %v", c.serverList, errS)
		c.dbSession = nil
		return errS
	}

	if c.keyspace != nil {
		c.keyspace.dbSession = c.dbSession
	} else {
		c.keyspace = GetKeySpace(c.keyspaceName, c.dbSession)
		c.keyspace.dbSession = c.dbSession
	}

	return nil
}

// Disconnect removes the connectivity to Cassandra cluster
func (c *Client) Disconnect() error {
	if c.Isconnected() {
		c.dbSession.Close()
	}
	return nil
}

func (c *Client) Isconnected() bool {
	return c.dbSession != nil
}

func (c *Client) ReConnect() error {
	var errS error

	atomic.AddInt64(&c.reconnectCtr, 1)

	c.Disconnect() // ignore error

	c.dbSession, errS = c.clusterCfg.CreateSession()
	if errS != nil {
		log.Printf("error creating Cassandra session to cluster: ", c.serverList,
			" :: ", errS)
		c.dbSession = nil
		return errS
	}

	return nil
}

func (c *Client) GetDB() (ops.Database, error) {

	if c.dbSession == nil {
		return nil, errors.New("No connections found. First connect to database server before calling this method")
	}

	if c.keyspace == nil {
		return nil, errors.New("No database found. First set database name before getting database")
	}

	if c.keyspace.Name == "" {
		return nil, errors.New("No database found. First set database name before getting database")
	}

	return c.keyspace, nil
}

func (c *Client) SetDBName(name string) error {
	if c.clusterCfg.Keyspace == name {
		return nil
	}

	c.dbSession.Close()
	c.clusterCfg.Keyspace = name
	var errS error
	c.dbSession, errS = c.clusterCfg.CreateSession()
	return errS
}

// CreateDB creates a keyspace in Cassandra given the name of the
// keyspace and the gocql session
func (c *Client) CreateDB(name string) error {
	// already exists
	if ok, err := c.DoesDBExist(name); err == nil {
		if ok {
			return nil
		}
	}

	ksStr := fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = { 'class' : "+
		" 'SimpleStrategy', 'replication_factor' : 1 };", name)
	cassQuery, err := CreateQuery(c.dbSession, ksStr)
	if err != nil {
		return err
	}
	if err = ExecQuery(cassQuery); err != nil {
		log.Printf("could not create keyspace: %s :: %v", c.keyspace, err)
		return err
	}
	return nil
}

// DropDB is used to drop the cassandra keyspace
func (c *Client) DropDB(name string) error {
	dropStr := fmt.Sprintf("DROP KEYSPACE %s", name)
	cassQuery, err := CreateQuery(c.dbSession, dropStr)
	if err != nil {
		return err
	}
	if err = ExecQuery(cassQuery); err != nil {
		log.Printf("could not drop keyspace: %s :: %v", name, err)
	}
	log.Printf("dropped keyspace: %s", name)
	return nil
}

//  Returns a list of databases in the Cassandra server list this client points to
func (c *Client) ListDBs() ([]string, error) {
	if c.dbSession == nil {
		return nil, errors.New("No valid session found")
	}
	var name string
	keyspaces := []string{}
	iter := c.dbSession.Query(`SELECT keyspace_name FROM ` +
		`system_schema.keyspaces`).Iter()
	for iter.Scan(&name) {
		keyspaces = append(keyspaces, name)
	}
	return keyspaces, nil
}

//  Checks to see if a given cassandra keyspace exists.
func (c *Client) DoesDBExist(name string) (bool, error) {
	if c.dbSession == nil {
		return false, errors.New("No valid session found")
	}
	var keyspace string
	iter := c.dbSession.Query(`SELECT keyspace_name FROM ` +
		`system_schema.keyspaces`).Iter()
	for iter.Scan(&keyspace) {
		if name == keyspace {
			log.Printf("keySpace %s exists", name)
			return true, nil
		}
	}
	return false, nil
}
