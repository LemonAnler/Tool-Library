package md5

import (
	"crypto/md5"
	"encoding/hex"
)

func String(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
