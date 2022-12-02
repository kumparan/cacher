package cacher

import (
	"encoding/json"
	"fmt"
	"strconv"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/kumparan/redsync/v4"
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

func get(client redigo.Conn, key string, counterKey string) (value any, counter int, err error) {
	defer func() {
		_ = client.Close()
	}()

	err = client.Send("MULTI")
	if err != nil {
		return nil, 0, err
	}
	err = client.Send("EXISTS", key)
	if err != nil {
		return nil, 0, err
	}
	err = client.Send("GET", key)
	if err != nil {
		return nil, 0, err
	}
	err = client.Send("GET", counterKey)
	if err != nil {
		return nil, 0, err
	}
	res, err := redigo.Values(client.Do("EXEC"))
	if err != nil {
		return nil, 0, err
	}

	val, ok := res[0].(int64)
	if !ok || val <= 0 {
		return nil, 0, ErrKeyNotExist
	}

	bt, _ := res[2].([]byte)
	err = json.Unmarshal(bt, &counter)
	if err != nil {
		return res[1], 0, nil
	}

	return res[1], counter, nil
}

func getHashMember(client redigo.Conn, identifier, key string) (value any, err error) {
	defer func() {
		_ = client.Close()
	}()

	err = client.Send("MULTI")
	if err != nil {
		return nil, err
	}
	err = client.Send("HEXISTS", identifier, key)
	if err != nil {
		return nil, err
	}
	err = client.Send("HGET", identifier, key)
	if err != nil {
		return nil, err
	}
	res, err := redigo.Values(client.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	val, ok := res[0].(int64)
	if !ok || val <= 0 {
		return nil, ErrKeyNotExist
	}

	return res[1], nil
}

func generateCacheHitKey(key string) string {
	return fmt.Sprintf("%s:cache:hit:counter", key)
}
