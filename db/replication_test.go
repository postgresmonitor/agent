package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindHostAndApplicationNameForPrimaryFromConnInfoEmpty(t *testing.T) {
	monitor := &ReplicationMonitor{}

	serverHost, applicationName := monitor.FindHostAndApplicationNameForPrimaryFromConnInfo("")
	assert.Empty(t, serverHost)
	assert.Empty(t, applicationName)
}

func TestFindHostAndApplicationNameForPrimaryFromConnInfoMissing(t *testing.T) {
	monitor := &ReplicationMonitor{}

	serverHost, applicationName := monitor.FindHostAndApplicationNameForPrimaryFromConnInfo("user=postgres passfile=/etc/postgresql/recovery_pgpass")
	assert.Empty(t, serverHost)
	assert.Empty(t, applicationName)
}

func TestFindHostAndApplicationNameForPrimaryFromConnInfoValid(t *testing.T) {
	monitor := &ReplicationMonitor{}

	serverHost, applicationName := monitor.FindHostAndApplicationNameForPrimaryFromConnInfo("user=postgres passfile=/etc/postgresql/recovery_pgpass channel_binding=prefer dbname=replication host=ec2-123-456-789.compute-1.amazonaws.com port=5432 application_name=follower fallback_application_name=walreceiver sslmode=prefer sslcompression=0 sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=prefer krbsrvname=postgres target_session_attrs=any")
	assert.Equal(t, "ec2-123-456-789.compute-1.amazonaws.com", serverHost)
	assert.Equal(t, "follower", applicationName)
}
