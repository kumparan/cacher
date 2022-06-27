package cacher

import (
	"strconv"

	"github.com/go-redsync/redsync/v4"
)

// SafeUnlock safely unlock mutex
func SafeUnlock(mutex *redsync.Mutex) {
	if mutex != nil {
		_, _ = mutex.Unlock()
	}
}

// parse result return from scan
// the index 0 is the cursor
// and the rest is the elements
func parseScanResults(results []any) (cursor int64, elements []string, err error) {
	if len(results) != 2 {
		return
	}

	cursor, err = strconv.ParseInt(string(results[0].([]byte)), 10, 64)
	if err != nil {
		return
	}

	elementsInterface := results[1].([]any)
	elements = make([]string, len(elementsInterface))
	for index, keyInterface := range elementsInterface {
		elements[index] = string(keyInterface.([]byte))
	}

	return
}

// getOffset to get offset from page and limit, min value for page = 1
func getOffset(page, limit int64) int64 {
	offset := (page - 1) * limit
	if offset < 0 {
		return 0
	}
	return offset
}
