package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildPgBouncerURLEmpty(t *testing.T) {
	monitor := &PgBouncerMonitor{}
	url := monitor.BuildPgBouncerURL("")
	assert.Empty(t, url)
}

func TestBuildPgBouncerURL(t *testing.T) {
	monitor := &PgBouncerMonitor{}
	url := monitor.BuildPgBouncerURL("postgres://user:pass@ec2-123-456-789.compute-1.amazonaws.com:5432/database")
	assert.Equal(t, "postgres://user:pass@ec2-123-456-789.compute-1.amazonaws.com:5433/pgbouncer?application_name=postgres-monitor-agent&statement_cache_mode=describe&prefer_simple_protocol=true", url)
}

func TestPgBouncerStatsDelta(t *testing.T) {
	o := &PgBouncerStats{
		TransactionCount: 10,
		QueryCount:       5,
		BytesReceived:    1,
		BytesSent:        1,
		TransactionTime:  1,
		QueryTime:        10,
		WaitTime:         5,
	}

	l := &PgBouncerStats{
		TransactionCount: 10,
		QueryCount:       5,
		BytesReceived:    2,
		BytesSent:        2,
		TransactionTime:  2,
		QueryTime:        15,
		WaitTime:         6,
	}

	d := o.Delta(l)

	assert.Equal(t, float64(0), d.TransactionCount)
	assert.Equal(t, float64(0), d.QueryCount)
	assert.Equal(t, float64(1), d.BytesReceived)
	assert.Equal(t, float64(1), d.BytesSent)
	assert.Equal(t, float64(1), d.TransactionTime)
	assert.Equal(t, float64(5), d.QueryTime)
	assert.Equal(t, float64(1), d.WaitTime)
}
