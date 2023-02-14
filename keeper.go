package cacher

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/jpillora/backoff"
	"github.com/kumparan/redsync/v4"
	redigosync "github.com/kumparan/redsync/v4/redis/redigo"
)

const (
	// Override these when constructing the cache keeper
	defaultTTL                  = 10 * time.Second
	defaultNilTTL               = 5 * time.Minute
	defaultLockDuration         = 1 * time.Minute
	defaultWaitTime             = 15 * time.Second
	defaultMaxCacheTTL          = 48 * time.Hour
	defaultMinCacheTTLThreshold = 5 * time.Second
	defaultLockTries            = 1
	defaultCacheHitThreshold    = 10
	defaultMultiplierFactor     = 2
)

var nilValue = []byte("null")

type (
	// GetterFn :nodoc:
	GetterFn func() (any, error)

	// Keeper responsible for managing cache
	Keeper interface {
		Get(key string) (any, error)
		GetOrLock(key string) (any, *redsync.Mutex, error)
		GetOrSet(key string, fn GetterFn, opts ...func(Item)) ([]byte, error)
		Store(*redsync.Mutex, Item) error
		StoreWithoutBlocking(Item) error
		StoreMultiWithoutBlocking([]Item) error
		StoreMultiPersist([]Item) error
		StoreNil(cacheKey string) error
		Expire(string, time.Duration) error
		ExpireMulti(map[string]time.Duration) error
		Purge(string) error
		DeleteByKeys([]string) error
		IncreaseCachedValueByOne(key string) error

		AcquireLock(string) (*redsync.Mutex, error)
		SetDefaultTTL(time.Duration)
		SetNilTTL(time.Duration)
		SetConnectionPool(*redigo.Pool)
		SetLockConnectionPool(*redigo.Pool)
		SetLockDuration(time.Duration)
		SetLockTries(int)
		SetWaitTime(time.Duration)
		SetDisableCaching(bool)
		SetEnableDynamicTTL(bool)
		SetMaxCacheTTL(time.Duration)
		SetMinCacheTTLThreshold(time.Duration)
		SetCacheHitThreshold(int64)
		SetMultiplierFactor(int64)

		CheckKeyExist(string) (bool, error)

		//list
		StoreRightList(string, any) error
		StoreLeftList(string, any) error
		GetList(string, int64, int64) (any, error)
		GetListLength(string) (int64, error)
		GetAndRemoveFirstListElement(string) (any, error)
		GetAndRemoveLastListElement(string) (any, error)

		GetTTL(string) (int64, error)

		// HASH BUCKET
		GetHashMemberOrLock(identifier string, key string) (any, *redsync.Mutex, error)
		GetHashMemberOrSet(identifier, key string, fn GetterFn, opts ...func(Item)) ([]byte, error)
		StoreHashMember(string, Item) error
		StoreHashNilMember(identifier, cacheKey string) error
		GetHashMember(identifier string, key string) (any, error)
		DeleteHashMember(identifier string, key string) error
		IncreaseHashMemberValue(identifier, key string, value int64) (int64, error)
		GetHashMemberThenDelete(identifier, key string) (any, error)
		HashScan(identifier string, cursor int64) (next int64, result map[string]string, err error)
	}

	keeper struct {
		connPool             *redigo.Pool
		nilTTL               time.Duration
		defaultTTL           time.Duration
		waitTime             time.Duration
		maxCacheTTL          time.Duration
		minCacheTTLThreshold time.Duration
		disableCaching       bool
		enableDynamicTTL     bool
		multiplierFactor     int64

		lockConnPool      *redigo.Pool
		lockDuration      time.Duration
		lockTries         int
		cacheHitThreshold int64
	}
)

// NewKeeper :nodoc:
func NewKeeper() Keeper {
	return &keeper{
		defaultTTL:           defaultTTL,
		nilTTL:               defaultNilTTL,
		lockDuration:         defaultLockDuration,
		lockTries:            defaultLockTries,
		waitTime:             defaultWaitTime,
		disableCaching:       false,
		enableDynamicTTL:     false,
		cacheHitThreshold:    defaultCacheHitThreshold,
		maxCacheTTL:          defaultMaxCacheTTL,
		minCacheTTLThreshold: defaultMinCacheTTLThreshold,
		multiplierFactor:     defaultMultiplierFactor,
	}
}

// SetDefaultTTL :nodoc:
func (k *keeper) SetDefaultTTL(d time.Duration) {
	k.defaultTTL = d
}

// SetMultiplierFactor :nodoc:
func (k *keeper) SetMultiplierFactor(d int64) {
	k.multiplierFactor = d
}

// SetMaxCacheTTL maximum TTL allowed after extended. Only being used if Dynamic Cache is enabled
func (k *keeper) SetMaxCacheTTL(d time.Duration) {
	k.maxCacheTTL = d
}

// SetMinCacheTTLThreshold if current TTL is below this threshold, then the TTL won't be extended.
// Only being used if Dynamic Cache is enabled
func (k *keeper) SetMinCacheTTLThreshold(d time.Duration) {
	k.maxCacheTTL = d
}

// SetCacheHitThreshold is the threshold before the cache is extended. If the counter hasn't reached the threshold, it won't be extended.
// Only being used if Dynamic Cache is enabled
func (k *keeper) SetCacheHitThreshold(d int64) {
	k.cacheHitThreshold = d
}

func (k *keeper) SetNilTTL(d time.Duration) {
	k.nilTTL = d
}

// SetConnectionPool :nodoc:
func (k *keeper) SetConnectionPool(c *redigo.Pool) {
	k.connPool = c
}

// SetLockConnectionPool :nodoc:
func (k *keeper) SetLockConnectionPool(c *redigo.Pool) {
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

// SetEnableDynamicTTL :nodoc:
func (k *keeper) SetEnableDynamicTTL(b bool) {
	k.enableDynamicTTL = b
}

// Get :nodoc:
func (k *keeper) Get(key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, ttl, err := get(k.connPool.Get(), key)
	switch err {
	case nil, ErrKeyNotExist, redigo.ErrNil:
	default:
		return nil, err
	}
	if cachedItem != nil {
		if k.enableDynamicTTL {
			k.extendCacheTTL(key, ttl)
		}
		return cachedItem, nil
	}

	return nil, nil
}

// GetOrLock :nodoc:
func (k *keeper) GetOrLock(key string) (cachedItem any, mutex *redsync.Mutex, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.Get(key)
	if err != nil || cachedItem != nil {
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
			cachedItem, ttlValue, err := get(k.connPool.Get(), key)
			if err != nil {
				if err == ErrKeyNotExist {
					mutex, err = k.AcquireLock(key)
					if err == nil {
						return nil, mutex, nil
					}

					goto Wait
				}
				return nil, nil, err
			}
			if k.enableDynamicTTL {
				k.extendCacheTTL(key, ttlValue)
			}
			return cachedItem, nil, nil
		}

	Wait:
		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, ErrWaitTooLong
}

// GetOrSet :nodoc:
func (k *keeper) GetOrSet(key string, fn GetterFn, opts ...func(Item)) (res []byte, err error) {
	cachedValue, mu, err := k.GetOrLock(key)
	if err != nil {
		return
	}
	if cachedValue != nil {
		res, ok := cachedValue.([]byte)
		if !ok {
			return nil, errors.New("invalid cache value")
		}

		return res, nil
	}

	// handle if nil value is cached
	if mu == nil {
		return
	}

	defer SafeUnlock(mu)
	item, err := fn()
	if err != nil {
		return
	}

	if item == nil {
		_ = k.StoreNil(key)
		return
	}

	cachedValue, err = json.Marshal(item)
	if err != nil {
		return
	}

	cacheItem := NewItem(key, cachedValue)
	for _, o := range opts {
		o(cacheItem)
	}
	_ = k.Store(mu, cacheItem)
	return cachedValue.([]byte), nil
}

// Store :nodoc:
func (k *keeper) Store(mutex *redsync.Mutex, c Item) error {
	if k.disableCaching {
		return nil
	}
	defer SafeUnlock(mutex)

	return k.StoreWithoutBlocking(c)
}

// StoreWithoutBlocking :nodoc:
func (k *keeper) StoreWithoutBlocking(c Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		return err
	}

	err = client.Send("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
	if err != nil {
		return err
	}

	if k.enableDynamicTTL {
		// set counter cache to 0 with the same TTL as the main cache key
		err = client.Send("SETEX", getCounterKey(c.GetKey()), k.decideCacheTTL(c), 0)
		if err != nil {
			return err
		}
	}

	_, err = client.Do("EXEC")
	return err
}

// StoreNil :nodoc:
func (k *keeper) StoreNil(cacheKey string) error {
	item := NewItemWithCustomTTL(cacheKey, nilValue, k.nilTTL)
	err := k.StoreWithoutBlocking(item)
	return err
}

// StoreHashNilMember :nodoc:
func (k *keeper) StoreHashNilMember(identifier, cacheKey string) error {
	item := NewItemWithCustomTTL(cacheKey, nilValue, k.nilTTL)
	err := k.StoreHashMember(identifier, item)
	return err
}

// Purge :nodoc:
func (k *keeper) Purge(matchString string) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	var cursor any
	var stop []uint8
	cursor = "0"
	delCount := 0
	for {
		res, err := redigo.Values(client.Do("SCAN", cursor, "MATCH", matchString, "COUNT", 500000))
		if err != nil {
			return err
		}
		stop = res[0].([]uint8)
		if foundKeys, ok := res[1].([]any); ok {
			if len(foundKeys) > 0 {
				err = client.Send("DEL", foundKeys...)
				if err != nil {
					return err
				}
				delCount++
			}

			// ascii for '0' is 48
			if stop[0] == 48 {
				break
			}
		}

		cursor = res[0]
	}
	if delCount > 0 {
		_ = client.Flush()
	}
	return nil
}

// IncreaseCachedValueByOne will increment the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) IncreaseCachedValueByOne(key string) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err := client.Do("INCR", key)
	return err
}

// AcquireLock :nodoc:
func (k *keeper) AcquireLock(key string) (*redsync.Mutex, error) {
	p := redigosync.NewPool(k.lockConnPool)
	r := redsync.New(p)
	m := r.NewMutex("lock:"+key,
		redsync.WithExpiry(k.lockDuration),
		redsync.WithTries(k.lockTries))

	return m, m.Lock()
}

// DeleteByKeys Delete by multiple keys
func (k *keeper) DeleteByKeys(keys []string) error {
	if k.disableCaching {
		return nil
	}

	if len(keys) <= 0 {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	var redisKeys []any
	for _, key := range keys {
		redisKeys = append(redisKeys, key, getCounterKey(key))
	}

	_, err := client.Do("DEL", redisKeys...)
	return err
}

// StoreMultiWithoutBlocking Store multiple items
func (k *keeper) StoreMultiWithoutBlocking(items []Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		return err
	}
	for _, item := range items {
		err = client.Send("SETEX", item.GetKey(), k.decideCacheTTL(item), item.GetValue())
		if err != nil {
			return err
		}
	}

	_, err = client.Do("EXEC")
	return err
}

// StoreMultiPersist Store multiple items with persistence
func (k *keeper) StoreMultiPersist(items []Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		return err
	}
	for _, item := range items {
		err = client.Send("SET", item.GetKey(), item.GetValue())
		if err != nil {
			return err
		}
		err = client.Send("PERSIST", item.GetKey())
		if err != nil {
			return err
		}
	}

	_, err = client.Do("EXEC")
	return err
}

// Expire Set expire a key
func (k *keeper) Expire(key string, duration time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err = client.Do("EXPIRE", key, int64(duration.Seconds()))
	return
}

// ExpireMulti Set expire multiple
func (k *keeper) ExpireMulti(items map[string]time.Duration) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		return err
	}
	for k, duration := range items {
		err = client.Send("EXPIRE", k, int64(duration.Seconds()))
		if err != nil {
			return err
		}
	}

	_, err = client.Do("EXEC")
	return err
}

// CheckKeyExist :nodoc:
func (k *keeper) CheckKeyExist(key string) (value bool, err error) {

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	val, err := client.Do("EXISTS", key)
	res, ok := val.(int64)
	if ok && res > 0 {
		value = true
	}

	return
}

// StoreRightList :nodoc:
func (k *keeper) StoreRightList(name string, value any) error {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err := client.Do("RPUSH", name, value)

	return err
}

// StoreLeftList :nodoc:
func (k *keeper) StoreLeftList(name string, value any) error {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err := client.Do("LPUSH", name, value)

	return err
}

func (k *keeper) GetListLength(name string) (value int64, err error) {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	val, err := client.Do("LLEN", name)
	value = val.(int64)

	return
}

func (k *keeper) GetAndRemoveFirstListElement(name string) (value any, err error) {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = client.Do("LPOP", name)
	return
}

func (k *keeper) GetAndRemoveLastListElement(name string) (value any, err error) {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = client.Do("RPOP", name)
	return
}

func (k *keeper) GetList(name string, size int64, page int64) (value any, err error) {
	offset := getOffset(page, size)

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	llen, err := k.GetListLength(name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	end := offset + size

	value, err = client.Do("LRANGE", name, offset, end)
	return
}

func (k *keeper) GetTTL(name string) (value int64, err error) {
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	val, err := client.Do("TTL", name)
	if err != nil {
		return
	}

	value = val.(int64)
	return
}

// StoreHashMember :nodoc:
func (k *keeper) StoreHashMember(identifier string, c Item) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err = client.Send("MULTI")
	if err != nil {
		return err
	}
	_, err = client.Do("HSET", identifier, c.GetKey(), c.GetValue())
	if err != nil {
		return err
	}
	_, err = client.Do("EXPIRE", identifier, k.decideCacheTTL(c))
	if err != nil {
		return err
	}

	_, err = client.Do("EXEC")
	return
}

// GetHashMemberOrLock :nodoc:
func (k *keeper) GetHashMemberOrLock(identifier string, key string) (cachedItem any, mutex *redsync.Mutex, err error) {
	if k.disableCaching {
		return
	}

	lockKey := fmt.Sprintf("%s:%s", identifier, key)

	cachedItem, err = k.GetHashMember(identifier, key)
	if err != nil && err != redigo.ErrNil && err != ErrKeyNotExist || cachedItem != nil {
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
			if err != nil {
				if err == ErrKeyNotExist {
					mutex, err = k.AcquireLock(lockKey)
					if err == nil {
						return nil, mutex, nil
					}

					goto Wait
				}
				return nil, nil, err
			}
			return cachedItem, nil, nil
		}

	Wait:
		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, ErrWaitTooLong
}

// GetHashMemberOrSet :nodoc:
func (k *keeper) GetHashMemberOrSet(identifier, key string, fn GetterFn, opts ...func(Item)) (res []byte, err error) {
	cachedValue, mu, err := k.GetHashMemberOrLock(identifier, key)
	if err != nil {
		return
	}
	if cachedValue != nil {
		res, ok := cachedValue.([]byte)
		if !ok {
			return nil, errors.New("invalid cache value")
		}

		return res, nil
	}

	// handle if nil value is cached
	if mu == nil {
		return
	}

	defer SafeUnlock(mu)
	item, err := fn()
	if err != nil {
		return
	}

	if item == nil {
		_ = k.StoreHashNilMember(identifier, key)
		return
	}

	cachedValue, err = json.Marshal(item)
	if err != nil {
		return
	}

	cacheItem := NewItem(key, cachedValue)
	for _, o := range opts {
		o(cacheItem)
	}
	_ = k.StoreHashMember(identifier, cacheItem)
	return cachedValue.([]byte), nil
}

// GetHashMember :nodoc:
func (k *keeper) GetHashMember(identifier string, key string) (value any, err error) {
	if k.disableCaching {
		return
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	return getHashMember(client, identifier, key)
}

// DeleteHashMember :nodoc:
func (k *keeper) DeleteHashMember(identifier string, key string) (err error) {
	if k.disableCaching {
		return
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err = client.Do("HDEL", identifier, key)
	return
}

// IncreaseHashMemberValue :nodoc:
func (k *keeper) IncreaseHashMemberValue(identifier, key string, value int64) (int64, error) {
	if k.disableCaching {
		return 0, nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	var count int64
	reply, err := client.Do("HINCRBY", identifier, key, value)
	if val, ok := reply.(int64); ok {
		count = val
	}

	return count, err
}

// GetHashMemberThenDelete :nodoc:
func (k *keeper) GetHashMemberThenDelete(identifier string, key string) (any, error) {
	if k.disableCaching {
		return nil, nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		return nil, err
	}

	err = client.Send("HGET", identifier, key)
	if err != nil {
		return nil, err
	}

	err = client.Send("HDEL", identifier, key)
	if err != nil {
		return nil, err
	}

	reply, err := redigo.Values(client.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	return reply[0], nil
}

// HashScan iterate hash member
func (k *keeper) HashScan(identifier string, cursor int64) (next int64, result map[string]string, err error) {
	if k.disableCaching {
		return
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	reply, err := redigo.Values(client.Do("HSCAN", identifier, cursor))
	if err != nil {
		return
	}

	next, parsed, err := parseScanResults(reply)
	result = make(map[string]string)
	for i := 0; i < len(parsed); i += 2 {
		result[parsed[i]] = parsed[i+1]

	}

	return
}

func (k *keeper) decideCacheTTL(c Item) (ttl int64) {
	if ttl = c.GetTTLInt64(); ttl > 0 {
		return
	}

	return int64(k.defaultTTL.Seconds())
}

// extendCacheTTL will increase cache based on traffic
// if the traffic reaches the threshold, it will extend the cache TTL
// will not return error as this should not disturb the main operation
func (k *keeper) extendCacheTTL(key string, ttl int64) {
	// if current TTL is below the minimum threshold, just ignore it
	if ttl < int64(k.minCacheTTLThreshold.Seconds()) {
		return
	}

	counterKey := getCounterKey(key)
	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	res, err := client.Do("INCR", counterKey)
	if err != nil {
		logrus.Error(err)
		return
	}
	counterValue, ok := res.(int64)
	if !ok || counterValue <= 0 {
		return
	}

	// only increase TTL if the counter reaches threshold
	if counterValue%k.cacheHitThreshold != 0 {
		return
	}

	newTTL := ttl * k.multiplierFactor
	if newTTL > int64(k.maxCacheTTL) {
		newTTL = int64(k.maxCacheTTL)
	}
	err = client.Send("MULTI")
	if err != nil {
		logrus.Error(err)
		return
	}
	err = client.Send("EXPIRE", key, newTTL, "GT")
	if err != nil {
		logrus.Error(err)
		return
	}
	err = client.Send("EXPIRE", counterKey, newTTL, "GT")
	if err != nil {
		logrus.Error(err)
		return
	}

	_, err = client.Do("EXEC")
	if err != nil {
		logrus.Error(err)
		return
	}
}

func (k *keeper) isLocked(key string) bool {
	client := k.lockConnPool.Get()
	defer func() {
		_ = client.Close()
	}()

	reply, err := client.Do("GET", "lock:"+key)
	if err != nil || reply == nil {
		return false
	}

	return true
}

func getCounterKey(mainKey string) string {
	return fmt.Sprintf("%s:cache:hit:counter", mainKey)
}
