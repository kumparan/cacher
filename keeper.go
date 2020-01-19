package cacher

import (
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	goredis "github.com/go-redis/redis"
	"github.com/jpillora/backoff"
)

const (
	// Override these when constructing the cache keeper
	defaultTTL          = 10 * time.Second
	defaultNilTTL       = 5 * time.Minute
	defaultLockDuration = 1 * time.Minute
	defaultLockTries    = 1
	defaultWaitTime     = 15 * time.Second
)

var nilJSON = []byte("null")

type (
	// CacheGeneratorFn :nodoc:
	CacheGeneratorFn func() (interface{}, error)

	// Keeper responsible for managing cache
	Keeper interface {
		Get(string) (interface{}, error)
		GetOrLock(string) (interface{}, *redislock.Lock, error)
		GetOrSet(string, CacheGeneratorFn, time.Duration) (interface{}, error)
		Store(*redislock.Lock, Item) error
		StoreWithoutBlocking(Item) error
		StoreMultiWithoutBlocking([]Item) error
		StoreMultiPersist([]Item) error
		StoreNil(cacheKey string) error
		Expire(string, time.Duration) error
		ExpireMulti(map[string]time.Duration) error
		// Purge(string) error
		DeleteByKeys([]string) error
		IncreaseCachedValueByOne(key string) error

		AcquireLock(string) (*redislock.Lock, error)
		SetDefaultTTL(time.Duration)
		SetNilTTL(time.Duration)
		SetConnectionPool(*goredis.UniversalClient)
		SetLockConnectionPool(*goredis.UniversalClient)
		SetLockDuration(time.Duration)
		SetLockTries(int)
		SetWaitTime(time.Duration)
		SetDisableCaching(bool)

		CheckKeyExist(string) (bool, error)

		//list
		StoreRightList(string, interface{}) error
		StoreLeftList(string, interface{}) error
		GetList(string, int64, int64) (interface{}, error)
		GetListLength(string) (int64, error)
		GetAndRemoveFirstListElement(string) (interface{}, error)
		GetAndRemoveLastListElement(string) (interface{}, error)

		GetTTL(string) (int64, error)

		// HASH BUCKET
		GetHashMemberOrLock(identifier string, key string) (interface{}, *redislock.Lock, error)
		StoreHashMember(string, Item) error
		GetHashMember(identifier string, key string) (interface{}, error)
		DeleteHashMember(identifier string, key string) error
	}

	keeper struct {
		connPool       *goredis.UniversalClient
		nilTTL         time.Duration
		defaultTTL     time.Duration
		waitTime       time.Duration
		disableCaching bool

		lockConnPool *goredis.UniversalClient
		lockDuration time.Duration
		lockTries    int
	}
)

// NewKeeper :nodoc:
func NewKeeper() Keeper {
	return &keeper{
		defaultTTL:     defaultTTL,
		nilTTL:         defaultNilTTL,
		lockDuration:   defaultLockDuration,
		lockTries:      defaultLockTries,
		waitTime:       defaultWaitTime,
		disableCaching: false,
	}
}

// Get :nodoc:
func (k *keeper) Get(key string) (cachedItem interface{}, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(key)
	if err != nil && err != goredis.Nil || cachedItem != nil {
		return
	}

	return nil, nil
}

// GetOrLock :nodoc:
func (k *keeper) GetOrLock(key string) (cachedItem interface{}, mutex *redislock.Lock, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(key)
	if err != nil && err != goredis.Nil || cachedItem != nil {
		return
	}

	mutex, err = k.AcquireLock(key)
	if err == nil {
		return
	}

	start := time.Now()
	for {
		b := &backoff.Backoff{
			Min:    20 * time.Millisecond,
			Max:    200 * time.Millisecond,
			Jitter: true,
		}

		if !k.isLocked(key) {
			cachedItem, err = k.getCachedItem(key)
			if err != nil && err != goredis.Nil || cachedItem != nil {
				return
			}
			return nil, nil, nil
		}

		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, errors.New("wait too long")
}

// GetOrSet :nodoc:
func (k *keeper) GetOrSet(key string, fn CacheGeneratorFn, ttl time.Duration) (cachedItem interface{}, err error) {
	cachedItem, mu, err := k.GetOrLock(key)
	if err != nil {
		return
	}
	if cachedItem != nil {
		return
	}

	defer func() {
		if mu != nil {
			mu.Release()
		}
	}()

	cachedItem, err = fn()

	if err != nil {
		return
	}

	err = k.Store(mu, NewItemWithCustomTTL(key, cachedItem, ttl))

	return
}

// Store :nodoc:
func (k *keeper) Store(mutex *redislock.Lock, c Item) error {
	if k.disableCaching {
		return nil
	}
	defer mutex.Release()

	_, err := k.connPool.Set(c.GetKey(), c.GetValue(), k.decideCacheTTL(c))
	return err
}

// StoreWithoutBlocking :nodoc:
func (k *keeper) StoreWithoutBlocking(c Item) error {
	if k.disableCaching {
		return nil
	}

	_, err := k.connPool.Set(c.GetKey(), c.GetValue(), k.decideCacheTTL(c))
	return err
}

// StoreNil :nodoc:
func (k *keeper) StoreNil(cacheKey string) error {
	item := NewItemWithCustomTTL(cacheKey, nilJSON, k.nilTTL)
	err := k.StoreWithoutBlocking(item)
	return err
}

// Purge :nodoc:
// func (k *keeper) Purge(matchString string) error {
// 	if k.disableCaching {
// 		return nil
// 	}

// 	client := k.connPool.Get()
//

// 	var cursor interface{}
// 	var stop []uint8
// 	cursor = "0"
// 	delCount := 0
// 	for {
// 		res, err := redigo.Values(client.Do("SCAN", cursor, "MATCH", matchString, "COUNT", 500000))
// 		if err != nil {
// 			return err
// 		}
// 		stop = res[0].([]uint8)
// 		if foundKeys, ok := res[1].([]interface{}); ok {
// 			if len(foundKeys) > 0 {
// 				err = client.Send("DEL", foundKeys...)
// 				if err != nil {
// 					return err
// 				}
// 				delCount++
// 			}

// 			// ascii for '0' is 48
// 			if stop[0] == 48 {
// 				break
// 			}
// 		}

// 		cursor = res[0]
// 	}
// 	if delCount > 0 {
// 		client.Flush()
// 	}
// 	return nil
// }

// IncreaseCachedValueByOne will increments the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) IncreaseCachedValueByOne(key string) error {
	if k.disableCaching {
		return nil
	}

	_, err := k.connPool.Incr(key)
	return err
}

// SetDefaultTTL :nodoc:
func (k *keeper) SetDefaultTTL(d time.Duration) {
	k.defaultTTL = d
}

func (k *keeper) SetNilTTL(d time.Duration) {
	k.nilTTL = d
}

// SetConnectionPool :nodoc:
func (k *keeper) SetConnectionPool(c *goredis.UniversalClient) {
	k.connPool = c
}

// SetLockConnectionPool :nodoc:
func (k *keeper) SetLockConnectionPool(c *goredis.UniversalClient) {
	k.lockConnPool = c
}

// SetLockDuration :nodoc:
func (k *keeper) SetLockDuration(d time.Duration) {
	k.lockDuration = d
}

// SetLockTries :nodoc:
func (k *keeper) SetLockTries(t int) {
	k.lockTries = t
}

// SetWaitTime :nodoc:
func (k *keeper) SetWaitTime(d time.Duration) {
	k.waitTime = d
}

// SetDisableCaching :nodoc:
func (k *keeper) SetDisableCaching(b bool) {
	k.disableCaching = b
}

// AcquireLock :nodoc:
func (k *keeper) AcquireLock(key string) (*redislock.Lock, error) {
	locker := redislock.New(k.lockConnPool)
	lock, err := locker.Obtain("lock:"+key, k.lockDuration, redislock.Options{
		RetryStrategy: redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), k.lockTries),
	})

	return lock, err
}

// DeleteByKeys Delete by multiple keys
func (k *keeper) DeleteByKeys(keys []string) error {
	if k.disableCaching {
		return nil
	}

	redisKeys := []interface{}{}
	for _, key := range keys {
		redisKeys = append(redisKeys, key)
	}

	_, err := k.connPool.Del(redisKeys...)
	return err
}

// StoreMultiWithoutBlocking Store multiple items
func (k *keeper) StoreMultiWithoutBlocking(items []Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool

	pipeline := client.Pipeline()
	for _, item := range items {
		err = pipeline.Set(item.GetKey(), item.GetValue(), k.decideCacheTTL(item)).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	return err
}

// StoreMultiPersist Store multiple items with persistence
func (k *keeper) StoreMultiPersist(items []Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool

	pipeline := client.Pipeline()
	for _, item := range items {
		err = pipeline.Set(item.GetKey(), item.GetValue()).Err()
		if err != nil {
			return err
		}
		err = pipeline.Persist(item.GetKey()).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	return err
}

// Expire Set expire a key
func (k *keeper) Expire(key string, duration time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()

	_, err = client.Do("EXPIRE", key, int64(duration.Seconds()))
	return
}

// ExpireMulti Set expire multiple
func (k *keeper) ExpireMulti(items map[string]time.Duration) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool

	pipeline := client.Pipeline()
	for k, duration := range items {
		err = pipeline.Expire(k, int64(duration.Seconds()))
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	return err
}

func (k *keeper) decideCacheTTL(c Item) (ttl int64) {
	if ttl = c.GetTTLInt64(); ttl > 0 {
		return
	}

	return int64(k.defaultTTL.Seconds())
}

func (k *keeper) getCachedItem(key string) (value interface{}, err error) {
	return k.connPool.Get(key).Result()
}

func (k *keeper) isLocked(key string) bool {
	client := k.lockConnPool

	reply, err := client.Get("lock:" + key).Result()
	if err != nil || reply == nil {
		return false
	}

	return true
}

// CheckKeyExist :nodoc:
func (k *keeper) CheckKeyExist(key string) (value bool, err error) {

	client := k.connPool

	val, err := client.Exists(key).Int64()

	value = false
	if val.(int64) > 0 {
		value = true
	}

	return
}

// StoreRightList :nodoc:
func (k *keeper) StoreRightList(name string, value interface{}) error {
	client := k.connPool

	_, err := client.RPush(name, value)

	return err
}

// StoreLeftList :nodoc:
func (k *keeper) StoreLeftList(name string, value interface{}) error {
	client := k.connPool

	_, err := client.LPush(name, value)

	return err
}

func (k *keeper) GetListLength(name string) (value int64, err error) {
	client := k.connPool

	val, err := client.LLen(name).Int64()
	value = val.(int64)

	return
}

func (k *keeper) GetAndRemoveFirstListElement(name string) (value interface{}, err error) {
	client := k.connPool

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = client.LPop(name).Result()
	return
}

func (k *keeper) GetAndRemoveLastListElement(name string) (value interface{}, err error) {
	client := k.connPool

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = client.RPop(name).Result()
	return
}

func (k *keeper) GetList(name string, size int64, page int64) (value interface{}, err error) {
	offset := getOffset(page, size)

	client := k.connPool

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	end := offset + size

	value, err = client.LRange(name, offset, end).Result()
	return
}

func (k *keeper) GetTTL(name string) (value int64, err error) {
	client := k.connPool

	val, err := client.TTL(name).String()
	if err != nil {
		return
	}

	value = val.(int64)
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

// StoreHashMember :nodoc:
func (k *keeper) StoreHashMember(identifier string, c Item) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.connPool

	pipeline = client.Pipeline()

	_, err = pipeline.HSet(identifier, c.GetKey(), c.GetValue())
	if err != nil {
		return err
	}
	_, err = pipeline.Expire(identifier, k.decideCacheTTL(c))
	if err != nil {
		return err
	}

	_, err = pipeline.Exec()
	return
}

// GetOrLockHash :nodoc:
func (k *keeper) GetHashMemberOrLock(identifier string, key string) (cachedItem interface{}, mutex *redislock.Lock, err error) {
	if k.disableCaching {
		return
	}

	lockKey := fmt.Sprintf("%s:%s", identifier, key)

	cachedItem, err = k.GetHashMember(identifier, key)
	if err != nil && err != goredis.Nil || cachedItem != nil {
		return
	}

	mutex, err = k.AcquireLock(lockKey)
	if err == nil {
		return
	}

	start := time.Now()
	for {
		b := &backoff.Backoff{
			Min:    20 * time.Millisecond,
			Max:    200 * time.Millisecond,
			Jitter: true,
		}

		if !k.isLocked(lockKey) {
			cachedItem, err = k.GetHashMember(identifier, key)
			if err != nil && err != goredis.Nil || cachedItem != nil {
				return
			}
			return nil, nil, nil
		}

		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, errors.New("wait too long")
}

// StoreHashMember :nodoc:
func (k *keeper) GetHashMember(identifier string, key string) (value interface{}, err error) {
	if k.disableCaching {
		return
	}

	client := k.connPool

	value, err = client.HGet(identifier, key).Result()
	return
}

// DeleteHashMember :nodoc:
func (k *keeper) DeleteHashMember(identifier string, key string) (err error) {
	if k.disableCaching {
		return
	}

	client := k.connPool

	_, err = client.HDel(identifier, key)
	return
}
