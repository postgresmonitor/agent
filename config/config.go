package config

import (
	"agent/logger"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const version = "0.0.1"

type Config struct {
	APIEndpoint string
	APIKey      string
	Environment string // development vs production
	Port        string
	UUID        uuid.UUID
	Version     string

	LogLevel        string
	LogPostgresLogs bool

	MonitorInterval           time.Duration
	MonitorSchemaInterval     time.Duration
	MonitorSettingsInterval   time.Duration
	MonitorQueryStatsInterval time.Duration

	MonitorPgBouncer    bool
	MonitorQueryStats   bool
	MonitorReplication  bool
	MonitorSchema       bool
	MonitorSettings     bool
	MonitorAgentQueries bool

	TestMode bool
}

// Creates a new Config based on env vars
func New() Config {
	// initialize UTC timezone
	os.Setenv("TZ", "UTC")

	apiKey := getEnvVar("POSTGRES_MONITOR_API_KEY", "")
	if apiKey == "" {
		logger.Error("Missing POSTGRES_MONITOR_API_KEY")
		os.Exit(1)
	}

	endpoint := getEnvVar("POSTGRES_MONITOR_API_URL", "https://api.postgresmonitor.com/agent/v1/report")
	environment := getEnvVar("AGENT_ENV", "production")
	port := getEnvVar("PORT", "8080")

	logLevel := getEnvVar("LOG_LEVEL", "info")
	logPostgresLogs := getEnvVarBool("LOG_POSTGRES_LOGS", false)
	monitorPgBouncer := getEnvVarBool("MONITOR_PGBOUNCER", true)
	monitorQueryStats := getEnvVarBool("MONITOR_QUERY_STATS", true)
	monitorReplication := getEnvVarBool("MONITOR_REPLICATION", true)
	monitorSchema := getEnvVarBool("MONITOR_SCHEMA", true)
	monitorSettings := getEnvVarBool("MONITOR_SETTINGS", true)
	monitorAgentQueries := getEnvVarBool("MONITOR_AGENT_QUERIES", false)

	return Config{
		APIEndpoint:               endpoint,
		APIKey:                    apiKey,
		Environment:               environment,
		Port:                      port,
		UUID:                      uuid.New(),
		Version:                   version,
		LogLevel:                  logLevel,
		LogPostgresLogs:           logPostgresLogs,
		MonitorInterval:           30 * time.Second, // if data is sent more frequently, the api will drop the data
		MonitorQueryStatsInterval: 1 * time.Minute,  // ^
		MonitorSchemaInterval:     15 * time.Minute, // ^
		MonitorSettingsInterval:   3 * time.Hour,    // ^
		MonitorPgBouncer:          monitorPgBouncer,
		MonitorQueryStats:         monitorQueryStats,
		MonitorReplication:        monitorReplication,
		MonitorSchema:             monitorSchema,
		MonitorSettings:           monitorSettings,
		MonitorAgentQueries:       monitorAgentQueries,
	}
}

func (c *Config) IsDevelopment() bool {
	return c.Environment == "development"
}

func (c *Config) IsProduction() bool {
	return c.Environment == "production"
}

func (c *Config) IsLogDebug() bool {
	return c.LogLevel == "debug"
}

func (c *Config) SetTestMode() {
	c.TestMode = true
}

func getEnvVar(name string, defaultValue string) string {
	envVar := os.Getenv(name)
	if envVar == "" {
		envVar = defaultValue
	}
	return envVar
}

func getEnvVarBool(name string, defaultValue bool) bool {
	value := getEnvVar(name, strconv.FormatBool(defaultValue))

	valueBool, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}
	return valueBool
}
