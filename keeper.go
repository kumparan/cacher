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
	// RedisConnector :Cluster or Client:
	RedisConnector interface {
		TxPipeline() goredis.Pipeliner

		Get(key string) *goredis.StringCmd
		Set(key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
		Incr(key string) *goredis.IntCmd
		SetNX(key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
		Eval(script string, keys []string, args ...interface{}) *goredis.Cmd
		EvalSha(sha1 string, keys []string, args ...interface{}) *goredis.Cmd
		ScriptExists(scripts ...string) *goredis.BoolSliceCmd
		ScriptLoad(script string) *goredis.StringCmd
		Del(keys ...string) *goredis.IntCmd
		HDel(key string, fields ...string) *goredis.IntCmd
		HGet(key, field string) *goredis.StringCmd
		HSet(key, field string, value interface{}) *goredis.BoolCmd
		TTL(key string) *goredis.DurationCmd
		RPush(key string, values ...interface{}) *goredis.IntCmd
		RPop(key string) *goredis.StringCmd
		LLen(key string) *goredis.IntCmd
		LPop(key string) *goredis.StringCmd
		LPush(key string, values ...interface{}) *goredis.IntCmd
		LRange(key string, start, stop int64) *goredis.StringSliceCmd
		Expire(key string, expiration time.Duration) *goredis.BoolCmd
		Exists(keys ...string) *goredis.IntCmd
		Do(args ...interface{}) *goredis.Cmd
		Persist(key string) *goredis.BoolCmd
	}

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
		SetConnectionPool(RedisConnector)
		SetLockConnectionPool(RedisConnector)
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
		connPool       RedisConnector
		nilTTL         time.Duration
		defaultTTL     time.Duration
		waitTime       time.Duration
		disableCaching bool

		lockConnPool RedisConnector
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

// SetDefaultTTL :nodoc:
func (k *keeper) SetDefaultTTL(d time.Duration) {
	k.defaultTTL = d
}

func (k *keeper) SetNilTTL(d time.Duration) {
	k.nilTTL = d
}

// SetConnectionPool :nodoc:
func (k *keeper) SetConnectionPool(c RedisConnector) {
	k.connPool = c
}

// SetLockConnectionPool :nodoc:
func (k *keeper) SetLockConnectionPool(c RedisConnector) {
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
	defer func() {
		_ = mutex.Release()
	}()

	err := k.connPool.Set(c.GetKey(), c.GetValue(), k.decideCacheTTL(c)).Err()
	return err
}

// StoreWithoutBlocking :nodoc:
func (k *keeper) StoreWithoutBlocking(c Item) error {
	if k.disableCaching {
		return nil
	}

	_, err := k.connPool.Set(c.GetKey(), c.GetValue(), k.decideCacheTTL(c)).Result()
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

	_, err := k.connPool.Incr(key).Result()
	return err
}

// AcquireLock :nodoc:
func (k *keeper) AcquireLock(key string) (*redislock.Lock, error) {
	locker := redislock.New(k.lockConnPool)
	lock, err := locker.Obtain("lock:"+key, k.lockDuration, &redislock.Options{
		RetryStrategy: redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), k.lockTries),
	})

	return lock, err
}

// DeleteByKeys Delete by multiple keys
func (k *keeper) DeleteByKeys(keys []string) error {
	if k.disableCaching {
		return nil
	}

	var redisKeys []string
	for _, key := range keys {
		redisKeys = append(redisKeys, key)
	}

	_, err := k.connPool.Del(redisKeys...).Result()
	return err
}

// StoreMultiWithoutBlocking Store multiple items
func (k *keeper) StoreMultiWithoutBlocking(items []Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for _, item := range items {
		err = pipeline.Set(item.GetKey(), item.GetValue(), k.decideCacheTTL(item)).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	defer func() {
		err = pipeline.Close()
	}()
	return err
}

// StoreMultiPersist Store multiple items with persistence
func (k *keeper) StoreMultiPersist(items []Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for _, item := range items {
		err = pipeline.Set(item.GetKey(), item.GetValue(), 0).Err()
		if err != nil {
			return err
		}
		err = pipeline.Persist(item.GetKey()).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	defer func() {
		err = pipeline.Close()
	}()
	return err
}

// Expire Set expire a key
func (k *keeper) Expire(key string, duration time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	err = k.connPool.Expire(key, duration).Err()
	return
}

// ExpireMulti Set expire multiple
func (k *keeper) ExpireMulti(items map[string]time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for k, duration := range items {
		err = pipeline.Expire(k, duration).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec()
	defer func() {
		err = pipeline.Close()
	}()
	return err
}

func (k *keeper) decideCacheTTL(c Item) (ttl time.Duration) {
	if intTTL := c.GetTTLInt64(); intTTL > 0 {
		ttl = time.Duration(intTTL) * time.Second
		return
	}

	return k.defaultTTL
}

func (k *keeper) getCachedItem(key string) (value interface{}, err error) {
	if k.disableCaching {
		return
	}
	resp, err := k.connPool.Get(key).Result()
	if resp == "" {
		value = nil
		return
	}
	value = []byte(resp)

	return
}

func (k *keeper) isLocked(key string) bool {
	reply, err := k.lockConnPool.Get("lock:" + key).Result()
	if err != nil || reply == "" {
		return false
	}

	return true
}

// CheckKeyExist :nodoc:
func (k *keeper) CheckKeyExist(key string) (value bool, err error) {
	val, err := k.connPool.Exists(key).Result()

	value = false
	if val > 0 {
		value = true
	}

	return
}

// StoreRightList :nodoc:
func (k *keeper) StoreRightList(name string, value interface{}) error {
	return k.connPool.RPush(name, value).Err()
}

// StoreLeftList :nodoc:
func (k *keeper) StoreLeftList(name string, value interface{}) error {
	return k.connPool.LPush(name, value).Err()
}

func (k *keeper) GetListLength(name string) (value int64, err error) {
	return k.connPool.LLen(name).Result()
}

func (k *keeper) GetAndRemoveFirstListElement(name string) (value interface{}, err error) {
	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = k.connPool.LPop(name).Result()
	return
}

func (k *keeper) GetAndRemoveLastListElement(name string) (value interface{}, err error) {
	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = k.connPool.RPop(name).Result()
	return
}

func (k *keeper) GetList(name string, size int64, page int64) (value interface{}, err error) {
	offset := getOffset(page, size)

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	end := offset + size

	resp, err := k.connPool.LRange(name, offset, end).Result()
	if len(resp) == 0 {
		value = nil
		return
	}
	value = resp
	return
}

func (k *keeper) GetTTL(name string) (value int64, err error) {
	val, err := k.connPool.TTL(name).Result()
	if err != nil {
		return
	}

	value = int64(val.Seconds())
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

	pipeline := k.connPool.TxPipeline()

	err = pipeline.HSet(identifier, c.GetKey(), c.GetValue()).Err()
	if err != nil {
		return err
	}
	err = pipeline.Expire(identifier, k.decideCacheTTL(c)).Err()
	if err != nil {
		return err
	}

	_, err = pipeline.Exec()
	defer func() {
		err = pipeline.Close()
	}()
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
	resp, err := k.connPool.HGet(identifier, key).Result()
	if resp == "" {
		value = nil
		return
	}
	value = []byte(resp)
	return
}

// DeleteHashMember :nodoc:
func (k *keeper) DeleteHashMember(identifier string, key string) (err error) {
	if k.disableCaching {
		return
	}

	return k.connPool.HDel(identifier, key).Err()
}
