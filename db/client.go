package db

import (
	"agent/config"
	"agent/logger"
	"database/sql"
	nurl "net/url"
	"os"
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

	database string
	// used in matching pgbouncer metrics
	username string

	platform         string
	isAuroraPlatform bool
	isHerokuPlatform bool
	isRDSPlatform    bool

	maxConnections int64
	version        string

	pgBouncerEnabled              *bool
	pgBouncerMaxServerConnections int64
	pgBouncerVersion              string

	pgStatStatmentsExists *bool
}

type Client struct {
	config config.Config
	conn   *sql.DB
	dbURL  string
	mu     *sync.Mutex
}

func BuildPostgresClients(config config.Config) []*PostgresClient {
	var postgresClients []*PostgresClient

	// TODO: if config is within SSM then fetch those params or read them as env vars if injected directly

	// generate list of config var pairs of config name to value for vars ending in _URL with values starting with postgres://
	var configVars [][]string
	for _, e := range os.Environ() {
		configVar := strings.SplitN(e, "=", 2)
		if strings.HasSuffix(configVar[0], "_URL") && strings.HasPrefix(configVar[1], "postgres://") {
			configVars = append(configVars, configVar)
		}
	}

	for _, configVar := range configVars {
		postgresClient := NewPostgresClient(config, configVar)
		if postgresClient != nil {
			postgresClients = append(postgresClients, postgresClient)

			// if an aurora cluster url then also add a postgres client for the reader endpoint
			if config.DiscoverAuroraReaderEndpoint && postgresClient.isAuroraPlatform {
				readerPostgresClient := BuildDiscoveredAuroraReaderClient(config, configVars, configVar[0], postgresClient)
				if readerPostgresClient != nil {
					postgresClients = append(postgresClients, readerPostgresClient)
				}
			}
		}
	}

	return postgresClients
}

func NewPostgresClient(config config.Config, configVar []string) *PostgresClient {
	varName := configVar[0]
	url := configVar[1]

	var host string
	var database string
	var username string
	parsedUrl, err := nurl.Parse(url)
	if err != nil {
		logger.Error("Invalid URL! Missing host and database")
		host = ""
		database = ""
		username = ""
	} else {
		hostAndPort := strings.Split(parsedUrl.Host, ":")
		host = hostAndPort[0]
		database = strings.ReplaceAll(parsedUrl.Path, "/", "")

		if database == "" {
			logger.Error("Database is not configured for URL", "host", host)
		}

		username = parsedUrl.User.Username()
	}

	// set up appending more url params
	if strings.Contains(url, "?") {
		url += "&"
	} else {
		url += "?"
	}

	// set application name for db connections
	url += "application_name=postgres-monitor-agent"
	url += "&statement_cache_mode=describe" // don't use prepared statements since pgbouncer doesn't support it

	sqlClient := NewClient(config, url)
	if sqlClient.conn == nil {
		return nil
	}

	platform := GetPlatform(sqlClient, host)

	name := strings.ReplaceAll(varName, "_URL", "")

	// support both HEROKU_POSTGRESQL_BLUE_URL and BLUE_URL config vars
	if platform == HerokuPlatform {
		name = strings.ReplaceAll(name, "HEROKU_POSTGRESQL_", "")
	}

	postgresClient := &PostgresClient{
		client: sqlClient,
		serverID: &ServerID{
			Name:          name,
			ConfigVarName: varName,
			Database:      database,
		},
		url:      url,
		host:     host,
		database: database,
		username: username,
		platform: platform,
	}

	// platform specific settings
	if platform == AuroraPlatform {
		postgresClient.isAuroraPlatform = true
		postgresClient.serverID.Name = FindAuroraInstanceId(postgresClient)
	}

	if platform == HerokuPlatform {
		postgresClient.isHerokuPlatform = true
	}

	if platform == RDSPlatform {
		postgresClient.isRDSPlatform = true
		postgresClient.serverID.Name = ExtractRDSInstanceName(host)
	}

	// ensure the client is in a valid state with a valid URL
	if !postgresClient.IsValid() {
		return nil
	}

	return postgresClient
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
			defer conn.Close()
			logger.Error("Unable to connect to database", "err", err)
			return nil
		}
	}

	return conn
}

func (c *PostgresClient) SetPgBouncerEnabled(enabled bool) {
	c.pgBouncerEnabled = &enabled
}

func (c *PostgresClient) SetPgStatStatmentsExists(enabled bool) {
	c.pgStatStatmentsExists = &enabled
}

func (c *PostgresClient) IsValid() bool {
	return c.host != "" && c.serverID.Database != ""
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

// compare that URLs are equal ignoring url params
func ArePostgresURLsEqual(url string, otherURL string) bool {
	if url == otherURL {
		return true
	}

	// ignore query params
	url = strings.Split(url, "?")[0]
	otherURL = strings.Split(otherURL, "?")[0]

	return url == otherURL
}

func BuildDiscoveredAuroraReaderClient(config config.Config, configVars [][]string, envVarName string, writerPostgresClient *PostgresClient) *PostgresClient {
	if !IsAuroraClusterWriterHost(writerPostgresClient.host) {
		return nil
	}

	readerURL := GenerateAuroraClusterReaderURL(writerPostgresClient.url)
	// don't add reader host if the host is already configured through env vars
	var exists bool
	for _, existingConfigVar := range configVars {
		if ArePostgresURLsEqual(readerURL, existingConfigVar[1]) {
			exists = true
		}
	}

	if !exists {
		logger.Info("Trying possible Aurora reader cluster endpoint")
		// add aurora reader host postgres client
		readerPostgresClient := NewPostgresClient(config, []string{envVarName + "_READER", readerURL})

		if readerPostgresClient == nil {
			return nil
		}

		// make sure the reader's instance id isn't the same as the writer's instance id
		// since a singel writer instance will have the reader cluster endpoint redirect to the writer instance
		if readerPostgresClient.serverID.Name != writerPostgresClient.serverID.Name {
			return readerPostgresClient
		} else {
			logger.Info("No Aurora reader endpoint found - only a single writer instance in the cluster")
		}
	}

	return nil
}
