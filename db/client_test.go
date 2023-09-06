package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArePostgresURLsEqual(t *testing.T) {
	assert.True(t, ArePostgresURLsEqual("postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com", "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com"))
	assert.True(t, ArePostgresURLsEqual("postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com/db", "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com/db"))
	assert.True(t, ArePostgresURLsEqual("postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com:5432/db", "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com:5432/db?sslmode=require&application_name=postgres-monitor-agent&statement_cache_mode=describe"))

	assert.False(t, ArePostgresURLsEqual("postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com/db", "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com/db-2"))
	assert.False(t, ArePostgresURLsEqual("postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com:5432/db", "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com:5433/db"))
}
