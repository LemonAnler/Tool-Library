package md5

import (
	"crypto/md5"
	"encoding/hex"
	"os"
)

func String(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func PathString(path string) (string, error) {
	b, err := os.ReadFile(path)

	if err != nil {
		return "", err
	}

	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:]), nil
}
