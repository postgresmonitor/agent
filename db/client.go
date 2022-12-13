package db

import (
	"database/sql"
	"log"
	nurl "net/url"
	"os"
	"postgres-monitor/config"
	"postgres-monitor/logger"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v4/stdlib"
)

// Separating the sensitive client and URL state from the message struct below
// to ensure the URL and client are not leaked to other packages
type PostgresClient struct {
	// DB client
	client *Client

	serverID *ServerID

	// ex. postgres://<user>:<pass>@<host>:<port>/<db>
	url string

	// ex host from url
	host string

	platform string

	maxConnections int64
	version        string

	pgBouncerEnabled              *bool
	pgBouncerMaxServerConnections int64
	pgBouncerVersion              string
}

type Client struct {
	config config.Config
	conn   *sql.DB
	dbURL  string
	mu     *sync.Mutex
}

func BuildPostgresClients(config config.Config) []*PostgresClient {
	var postgresClients []*PostgresClient

	for _, e := range os.Environ() {
		configVar := strings.SplitN(e, "=", 2)
		if strings.HasSuffix(configVar[0], "_URL") && strings.HasPrefix(configVar[1], "postgres://") {
			postgresClient := NewPostgresClient(config, configVar)
			postgresClients = append(postgresClients, postgresClient)
		}
	}

	return postgresClients
}

func NewPostgresClient(config config.Config, configVar []string) *PostgresClient {
	varName := configVar[0]
	url := configVar[1]
	// support both HEROKU_POSTGRESQL_BLUE_URL and BLUE_URL config vars
	configName := strings.ReplaceAll(varName, "HEROKU_POSTGRESQL_", "")
	configName = strings.ReplaceAll(configName, "_URL", "")
	urlParts := strings.Split(url, "/")
	database := urlParts[len(urlParts)-1]

	var host string
	parsedUrl, err := nurl.Parse(url)
	if err != nil {
		log.Println("Invalid URL!")
		host = ""
	} else {
		hostAndPort := strings.Split(parsedUrl.Host, ":")
		host = hostAndPort[0]
	}

	// set application name for db connections
	url += "?application_name=postgres-monitor-agent"
	url += "&statement_cache_mode=describe" // don't use prepared statements since pgbouncer doesn't support it

	return &PostgresClient{
		client: NewClient(config, url),
		serverID: &ServerID{
			ConfigName:    configName,
			ConfigVarName: varName,
			Database:      database,
		},
		url:  url,
		host: host,
	}
}

func NewClient(config config.Config, dbURL string) *Client {
	return &Client{
		config: config,
		conn:   NewConn(dbURL, true),
		dbURL:  dbURL,
		mu:     &sync.Mutex{},
	}
}

func NewConn(dbURL string, testPing bool) *sql.DB {
	conn, err := sql.Open("pgx", dbURL)

	// test connection
	if err != nil {
		logger.Error("Unable to connect to database", "err", err)
		return nil
	}

	// restrict to a single connection to prevent opening too many connections
	conn.SetMaxOpenConns(1)

	// test connection - doesn't work with pgbouncer so make it configurable
	if testPing {
		if err := conn.Ping(); err != nil {
			logger.Error("Unable to connect to database", "err", err)
			return nil
		}
	}

	return conn
}

func (c *PostgresClient) SetPgBouncerEnabled(enabled bool) {
	c.pgBouncerEnabled = &enabled
}

// wrap pgx Query with mutex to ensure only one active connection is used at one time
func (c *Client) Query(query string) (*sql.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	query = CleanQuery(query)

	// We were seeing not all rows being returned by pgx - ex. FindIndexes was only returning 83 indexes
	// vs the 304 indexes that we were seeing in psql. Standard database/sql worked correctly when using pgx
	// as the backing driver.
	return c.conn.Query(query)
}

// wrap pgx QueryRow with mutex to ensure only one active connection is used at a time
func (c *Client) QueryRow(query string) *sql.Row {
	c.mu.Lock()
	defer c.mu.Unlock()

	query = CleanQuery(query)

	return c.conn.QueryRow(query)
}

// should only be used for very specific use cases
// ex. raising a test log message
func (c *Client) Exec(query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Exec(query)

	return err
}
