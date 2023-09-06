package db

import (
	"log"
	"os"
	"strings"
)

const (
	AuroraPlatform  = "aws_aurora"
	HerokuPlatform  = "heroku"
	RDSPlatform     = "aws_rds"
	UnknownPlatform = "unknown"
)

// use var form for testing
var GetPlatform = func(client *Client, host string) string {
	if IsHerokuPlatform(client) {
		return HerokuPlatform
	} else if IsAuroraPlatform(client) {
		return AuroraPlatform
	} else if IsRDSPlatform(client, host) {
		return RDSPlatform
	}
	return UnknownPlatform
}

func IsHerokuPlatform(client *Client) bool {
	if os.Getenv("DYNO") != "" {
		return true
	} else if HasHerokuSchema(client) {
		return true
	}

	return false
}

func HasHerokuSchema(client *Client) bool {
	var hasSchema bool

	query := "select 1 from information_schema.schemata where schema_name = 'heroku_ext'"

	err := client.QueryRow(query).Scan(&hasSchema)
	if err != nil {
		hasSchema = false
	}

	return hasSchema
}

func IsAuroraPlatform(client *Client) bool {
	if HasAuroraStatUtilsAvailableExtension(client) {
		return true
	}

	return false
}

func HasAuroraStatUtilsAvailableExtension(client *Client) bool {
	var hasAvailableExtension bool

	// could also use a specific aurora function like aurora_db_instance_identifier() or aurora_version()
	// but those may not be available on all aurora versions
	query := "select 1 from pg_available_extensions where name = 'aurora_stat_utils'" + postgresMonitorQueryComment()

	err := client.QueryRow(query).Scan(&hasAvailableExtension)
	if err != nil {
		hasAvailableExtension = false
	}

	return hasAvailableExtension
}

// test if host ends with rds.amazonaws.com and if not an aurora db
func IsRDSPlatform(client *Client, host string) bool {
	return strings.HasSuffix(host, "rds.amazonaws.com") && !IsAuroraPlatform(client)
}

func FindAuroraInstanceId(postgresClient *PostgresClient) string {
	var instanceId string

	// the aurora_db_instance_identifier is the DB instance name for the aurora db
	err := postgresClient.client.QueryRow("select * from aurora_db_instance_identifier()" + postgresMonitorQueryComment()).Scan(&instanceId)
	if err != nil {
		log.Println("Error: ", err)
		return ""
	}

	return instanceId
}

// see if host is for a cluster- aurora writer endpoint
// ex. test-db.cluster-abc12345.us-east-1.rds.amazonaws.com
func IsAuroraClusterWriterHost(host string) bool {
	subdomain := strings.Split(host, ".")[1]
	return strings.HasPrefix(subdomain, "cluster-") && !strings.HasPrefix(subdomain, "cluster-ro-")
}

func GenerateAuroraClusterReaderURL(writerURL string) string {
	// ex. "postgres://user:pass@test-db.cluster-abc12345.us-east-1.rds.amazonaws.com
	urlParts := strings.Split(writerURL, "@")
	host := urlParts[1]
	hostParts := strings.Split(host, ".")
	cluster := hostParts[1]
	clusterSuffix := strings.Split(cluster, "-")[1]
	readerCluster := "cluster-ro-" + clusterSuffix

	return strings.ReplaceAll(writerURL, cluster, readerCluster)
}

// ex. test-database-1.abc12345.us-east-1.rds.amazonaws.com => test-database-1
func ExtractRDSInstanceName(host string) string {
	return strings.Split(host, ".")[0]
}

func PlatformRequiresLogServer(platform string) bool {
	return platform == HerokuPlatform
}
