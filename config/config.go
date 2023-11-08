package config

import (
	"agent/logger"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const version = "0.0.7"

const (
	ECSAgentHostPlatform    = "aws_ecs"
	HerokuAgentHostPlatform = "heroku"
)

type Config struct {
	APIEndpoint       string
	APIKey            string
	Environment       string // development vs production
	Port              string
	AgentHostPlatform string
	UUID              uuid.UUID
	Version           string

	LogLevel        string
	LogPostgresLogs bool

	MonitorAWSLogsInterval           time.Duration
	MonitorCloudwatchMetricsInterval time.Duration
	MonitorCloudwatchLogsInterval    time.Duration
	MonitorInterval                  time.Duration
	MonitorSchemaInterval            time.Duration
	MonitorSettingsInterval          time.Duration
	MonitorQueryStatsInterval        time.Duration

	MonitorAgentQueries      bool
	MonitorAWSLogs           bool
	MonitorCloudwatchMetrics bool
	MonitorPgBouncer         bool
	MonitorQueryStats        bool
	MonitorReplication       bool
	MonitorSchema            bool
	MonitorSettings          bool

	DiscoverAuroraReaderEndpoint bool

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

	endpoint := getEnvVar("POSTGRES_MONITOR_API_URL", "https://agent.postgresmonitor.com/agent/v1/report")
	environment := getEnvVar("AGENT_ENV", "production")
	port := getEnvVar("PORT", "8080")

	logLevel := getEnvVar("LOG_LEVEL", "info")
	logPostgresLogs := getEnvVarBool("LOG_POSTGRES_LOGS", false)
	monitorAgentQueries := getEnvVarBool("MONITOR_AGENT_QUERIES", false)
	monitorAWSLogs := getEnvVarBool("MONITOR_AWS_LOGS", true)
	monitorCloudwatchMetrics := getEnvVarBool("MONITOR_CLOUDWATCH_METRICS", true)
	monitorPgBouncer := getEnvVarBool("MONITOR_PGBOUNCER", true)
	monitorQueryStats := getEnvVarBool("MONITOR_QUERY_STATS", true)
	monitorReplication := getEnvVarBool("MONITOR_REPLICATION", true)
	monitorSchema := getEnvVarBool("MONITOR_SCHEMA", true)
	monitorSettings := getEnvVarBool("MONITOR_SETTINGS", true)
	discoverAuroraReaderEndpoint := getEnvVarBool("AURORA_DISCOVER_READER_ENDPOINT", true)

	return Config{
		APIEndpoint:                      endpoint,
		APIKey:                           apiKey,
		Environment:                      environment,
		Port:                             port,
		AgentHostPlatform:                getAgentHostPlatform(),
		UUID:                             uuid.New(),
		Version:                          version,
		LogLevel:                         logLevel,
		LogPostgresLogs:                  logPostgresLogs,
		MonitorAWSLogsInterval:           2 * time.Minute,
		MonitorCloudwatchMetricsInterval: 5 * time.Minute,
		MonitorCloudwatchLogsInterval:    1 * time.Minute,
		MonitorInterval:                  30 * time.Second, // if data is sent more frequently, the api will drop the data
		MonitorQueryStatsInterval:        1 * time.Minute,  // ^
		MonitorSchemaInterval:            15 * time.Minute, // ^
		MonitorSettingsInterval:          3 * time.Hour,    // ^
		MonitorAgentQueries:              monitorAgentQueries,
		MonitorAWSLogs:                   monitorAWSLogs,
		MonitorCloudwatchMetrics:         monitorCloudwatchMetrics,
		MonitorPgBouncer:                 monitorPgBouncer,
		MonitorQueryStats:                monitorQueryStats,
		MonitorReplication:               monitorReplication,
		MonitorSchema:                    monitorSchema,
		MonitorSettings:                  monitorSettings,
		DiscoverAuroraReaderEndpoint:     discoverAuroraReaderEndpoint,
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

// use var form for testing
func getAgentHostPlatform() string {
	if os.Getenv("DYNO") != "" {
		return HerokuAgentHostPlatform
	} else if os.Getenv("ECS_CONTAINER_METADATA_URI") != "" || os.Getenv("ECS_CONTAINER_METADATA_URI_V4") != "" || os.Getenv("ECS_AGENT_URI") != "" {
		return ECSAgentHostPlatform
	} else {
		return ""
	}
}
