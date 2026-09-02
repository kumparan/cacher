package cacher

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/kumparan/go-utils"
	"github.com/sirupsen/logrus"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/kumparan/redsync/v4"
)

const (
	unlockMaxAttempts    = 2
	unlockRetryDelay     = 20 * time.Millisecond
	lockConcurrencyLimit = 10
	unlockBudget         = time.Second
	unlockAttemptTimeout = 250 * time.Millisecond
)

// SafeUnlock releases the given mutexes, concurrently and without ever
// releasing a lock that now belongs to someone else.
//
// It blocks until every mutex is released or unlockBudget elapses, whichever
// comes first, so callers that immediately retry a key do not race their own
// pending unlock. Mutexes that have already expired are skipped: the lock is
// gone either way and the round trip would be wasted.
//
// An unlock that reports success == false means the key was already gone. That
// is a normal outcome and is not retried. Only transport errors are retried.
func SafeUnlock(mutexes ...*redsync.Mutex) {
	live := make([]*redsync.Mutex, 0, len(mutexes))
	now := time.Now()
	for _, m := range mutexes {
		if m == nil {
			continue
		}
		// Until() is the zero time if the mutex was never acquired, and in the
		// past if the lock has already expired. Either way there is nothing of
		// ours left in redis to delete.
		if until := m.Until(); until.IsZero() || now.After(until) {
			logrus.WithField("mutex", m.Name()).Debug("skipping unlock, mutex already expired")
			continue
		}
		live = append(live, m)
	}

	if len(live) == 0 {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		eg := errgroup.Group{}
		eg.SetLimit(lockConcurrencyLimit)
		for _, m := range live {
			eg.Go(func() error {
				unlockWithRetry(m)
				return nil
			})
		}
		_ = eg.Wait()
	}()

	timer := time.NewTimer(unlockBudget)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		logrus.WithField("mutexes", len(live)).
			Warn("SafeUnlock exceeded its budget, remaining unlocks continue in the background")
	}
}

// unlockWithRetry releases a single mutex, retrying only on transport errors.
func unlockWithRetry(m *redsync.Mutex) {
	for attempt := 1; attempt <= unlockMaxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), unlockAttemptTimeout)
		unlocked, err := m.UnlockContext(ctx)
		cancel()

		if err == nil {
			if !unlocked {
				logrus.WithFields(logrus.Fields{
					"mutex":     m.Name(),
					"remaining": time.Until(m.Until()),
				}).Warn("mutex unlock didn't succeed, lock was already released or taken over")
			}
			return
		}

		logrus.WithFields(logrus.Fields{
			"mutex":     m.Name(),
			"attempt":   attempt,
			"remaining": time.Until(m.Until()),
		}).Error("failed to unlock mutex: ", err)

		if attempt == unlockMaxAttempts {
			return
		}

		// No point retrying a lock that expired while we were failing to
		// release it.
		if time.Now().After(m.Until()) {
			return
		}

		time.Sleep(unlockRetryDelay)
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

// StoreCachesWithCustomTTL store multiple object by keys with custom ttl
func StoreCachesWithCustomTTL[K comparable, V any](cacheKeeper Keeper, keys []K, buffer map[K]V, cacheKeyFunc func(K) string, ttl time.Duration) {
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

		cacheItems = append(cacheItems, NewItemWithCustomTTL(cacheKeyFunc(key), jsonVal, ttl))
	}

	err := cacheKeeper.StoreMultiWithoutBlocking(cacheItems)
	if err != nil {
		logger.WithField("cacheItems", utils.Dump(cacheItems)).Error(err)
	}
}
