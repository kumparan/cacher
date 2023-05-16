package cacher

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kumparan/go-utils"
	"github.com/sirupsen/logrus"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/kumparan/redsync/v4"
)

// SafeUnlock safely unlock mutex
func SafeUnlock(mutexes ...*redsync.Mutex) {
	for _, m := range mutexes {
		if m != nil {
			_, _ = m.Unlock()
		}
	}
}

// ParseCacheResultToPointerObject parse cache result to any object you want
func ParseCacheResultToPointerObject[T any](in any) (*T, error) {
	var obj *T
	by, ok := in.([]byte)
	if !ok {
		return nil, fmt.Errorf("failed to cast %T to byte", in)
	}

	err := json.Unmarshal(by, &obj)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s to %T", by, obj)
	}
	return obj, nil
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

func get(client redigo.Conn, key string) (value any, ttlValue int64, err error) {
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
	err = client.Send("TTL", key)
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

	ttlValue, ok = res[2].(int64)
	if !ok {
		return nil, 0, ErrInvalidTTL
	}

	return res[1], ttlValue, nil
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

// StoreCaches store multiple object by keys
func StoreCaches[K comparable, V any](cacheKeeper Keeper, keys []K, buffer map[K]V, cacheKeyFunc func(K) string) {
	logger := logrus.WithFields(logrus.Fields{
		"keys":   keys,
		"buffer": utils.Dump(buffer),
	})

	var cacheItems []Item
	for _, key := range keys {
		val, ok := buffer[key]
		if !ok {
			cacheItems = append(cacheItems, NewItem(cacheKeyFunc(key), []byte("null")))
			continue
		}

		jsonVal, err := json.Marshal(val)
		if err != nil {
			logger.WithField("key", key).Error(err)
			continue
		}

		cacheItems = append(cacheItems, NewItem(cacheKeyFunc(key), jsonVal))
	}

	err := cacheKeeper.StoreMultiWithoutBlocking(cacheItems)
	if err != nil {
		logger.WithField("cacheItems", utils.Dump(cacheItems)).Error(err)
	}
}
