package db

import (
	"agent/config"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObserver(t *testing.T) {
	config := config.Config{}
	postgresClient := &PostgresClient{
		client: &Client{
			config: config,
			dbURL:  "postgres://localhost:5432/test",
			mu:     &sync.Mutex{},
		},
		serverID: &ServerID{
			ConfigVarName: "GREEN_URL",
			ConfigName:    "GREEN",
		},
		url: "postgres://localhost:5432/test",
	}

	data := &Observer{
		config:          config,
		postgresClients: []*PostgresClient{postgresClient},
	}
	assert.NotNil(t, data)
}

func TestFindPostgresServersNoConfigVars(t *testing.T) {
	servers := BuildPostgresClients(config.Config{})
	assert.Empty(t, servers)
}

func TestFindPostgresServersInvalidConfigName(t *testing.T) {
	os.Setenv("GREEN_URI", "postgres://localhost:5432/test")

	servers := BuildPostgresClients(config.Config{})
	assert.Empty(t, servers)

	os.Unsetenv("GREEN_URI")
}

func TestFindPostgresServersInvalidURL(t *testing.T) {
	os.Setenv("GREEN_URL", "mysql://localhost:5432/test")

	servers := BuildPostgresClients(config.Config{})
	assert.Empty(t, servers)

	os.Unsetenv("GREEN_URL")
}

func TestFindPostgresServersConfigVar(t *testing.T) {
	os.Setenv("GREEN_URL", "postgres://localhost:5432/test")

	clients := BuildPostgresClients(config.Config{})
	assert.Equal(t, 1, len(clients))

	client := clients[0]
	assert.Equal(t, "GREEN_URL", client.serverID.ConfigVarName)
	assert.Equal(t, "GREEN", client.serverID.ConfigName)
	assert.Equal(t, "postgres://localhost:5432/test?application_name=postgres-monitor-agent&statement_cache_mode=describe", client.url)
	assert.NotNil(t, client.client)

	os.Unsetenv("GREEN_URL")
}

func TestFindPostgresServersConfigVarHerokuPostgres(t *testing.T) {
	os.Setenv("HEROKU_POSTGRESQL_GREEN_URL", "postgres://localhost:5432/test")

	clients := BuildPostgresClients(config.Config{})
	assert.Equal(t, 1, len(clients))

	client := clients[0]
	assert.Equal(t, "HEROKU_POSTGRESQL_GREEN_URL", client.serverID.ConfigVarName)
	assert.Equal(t, "GREEN", client.serverID.ConfigName)
	assert.Equal(t, "postgres://localhost:5432/test?application_name=postgres-monitor-agent&statement_cache_mode=describe", client.url)
	assert.NotNil(t, client.client)

	os.Unsetenv("HEROKU_POSTGRESQL_GREEN_URL")
}

func TestFindPostgresServersMultipleConfigVars(t *testing.T) {
	os.Setenv("GREEN_URL", "postgres://localhost:5432/test")
	os.Setenv("RED_URL", "postgres://localhost:5432/test2")

	clients := BuildPostgresClients(config.Config{})
	assert.Equal(t, 2, len(clients))

	green := clients[0]
	assert.Equal(t, "GREEN_URL", green.serverID.ConfigVarName)
	assert.Equal(t, "GREEN", green.serverID.ConfigName)
	assert.Equal(t, "postgres://localhost:5432/test?application_name=postgres-monitor-agent&statement_cache_mode=describe", green.url)
	assert.NotNil(t, green.client)
	assert.Equal(t, "test", green.serverID.Database)

	red := clients[1]
	assert.Equal(t, "RED_URL", red.serverID.ConfigVarName)
	assert.Equal(t, "RED", red.serverID.ConfigName)
	assert.Equal(t, "postgres://localhost:5432/test2?application_name=postgres-monitor-agent&statement_cache_mode=describe", red.url)
	assert.NotNil(t, red.client)
	assert.Equal(t, "test2", red.serverID.Database)

	os.Unsetenv("GREEN_URL")
	os.Unsetenv("RED_URL")
}
