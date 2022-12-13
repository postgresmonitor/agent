package db

import (
	"crypto/md5"
	"encoding/hex"
)

// take first 10 chars of md5 checksum for fingerprint
// query fingerprint is unique per database so this is fine
// obfuscated queries that differ in small ways (ex. # of values within an IN (1,2,3) clause)
// will have the same checksum because the obfuscated values are reduced to a single IN (?)
func fingerprintQuery(query string) string {
	checksum := md5.Sum([]byte(query))
	return hex.EncodeToString(checksum[:])[0:10]
}
