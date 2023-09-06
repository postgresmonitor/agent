package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAuroraClusterWriterHost(t *testing.T) {
	assert.True(t, IsAuroraClusterWriterHost("test-db.cluster-abc12345.us-east-1.rds.amazonaws.com"))
	assert.False(t, IsAuroraClusterWriterHost("test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com"))
	assert.False(t, IsAuroraClusterWriterHost("test-database-1.abc12345.us-east-1.rds.amazonaws.com"))
	assert.False(t, IsAuroraClusterWriterHost("ec2-123-456-78-90.compute-1.amazonaws.com"))
}

func TestExtractRDSInstanceName(t *testing.T) {
	assert.Equal(t, "test-database-1", ExtractRDSInstanceName("test-database-1.abc12345.us-east-1.rds.amazonaws.com"))
}

func TestGenerateAuroraClusterReaderURL(t *testing.T) {
	assert.Equal(t, "postgres://user:pass@test-db.cluster-ro-abc12345.us-east-1.rds.amazonaws.com", GenerateAuroraClusterReaderURL("postgres://user:pass@test-db.cluster-abc12345.us-east-1.rds.amazonaws.com"))
}
