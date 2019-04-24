package cacher

import (
	"errors"
	"time"

	"github.com/go-redsync/redsync"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/jpillora/backoff"
	"github.com/kumparan/go-lib/utils"
)

const (
	// Override these when constructing the cache keeper
	defaultTTL          = 10 * time.Second
	defaultLockDuration = 1 * time.Minute
	defaultLockTries    = 1
	defaultWaitTime     = 15 * time.Second
)

type (
	// CacheGeneratorFn :nodoc:
	CacheGeneratorFn func() (interface{}, error)

	// Keeper responsible for managing cache
	Keeper interface {
		Get(string) (interface{}, error)
		GetOrLock(string) (interface{}, *redsync.Mutex, error)
		GetOrSet(string, CacheGeneratorFn, time.Duration) (interface{}, error)
		Store(*redsync.Mutex, Item) error
		StoreWithoutBlocking(Item) error
		StoreMultiWithoutBlocking([]Item) error
		StoreMultiPersist([]Item) error
		Expire(string, time.Duration) error
		ExpireMulti(map[string]time.Duration) error
		Purge(string) error
		DeleteByKeys([]string) error
		IncreaseCachedValueByOne(key string) error

		AcquireLock(string) (*redsync.Mutex, error)
		SetDefaultTTL(time.Duration)
		SetConnectionPool(*redigo.Pool)
		SetLockConnectionPool(*redigo.Pool)
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
	}

	keeper struct {
		connPool       *redigo.Pool
		defaultTTL     time.Duration
		waitTime       time.Duration
		disableCaching bool

		lockConnPool *redigo.Pool
		lockDuration time.Duration
		lockTries    int
	}
)

// NewKeeper :nodoc:
func NewKeeper() Keeper {
	return &keeper{
		defaultTTL:     defaultTTL,
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
	if err != nil && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// GetOrLock :nodoc:
func (k *keeper) GetOrLock(key string) (cachedItem interface{}, mutex *redsync.Mutex, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(key)
	if err != nil && err != redigo.ErrNil || cachedItem != nil {
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
			if err != nil && err != redigo.ErrNil || cachedItem != nil {
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
			mu.Unlock()
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
func (k *keeper) Store(mutex *redsync.Mutex, c Item) error {
	if k.disableCaching {
		return nil
	}
	defer mutex.Unlock()

	client := k.connPool.Get()
	defer client.Close()

	_, err := client.Do("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
	return err
}

// StoreWithoutBlocking :nodoc:
func (k *keeper) StoreWithoutBlocking(c Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	_, err := client.Do("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
	return err
}

// Purge :nodoc:
func (k *keeper) Purge(matchString string) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	var cursor interface{}

	cursor = "0"
	for {
		res, err := redigo.Values(client.Do("SCAN", cursor, "MATCH", matchString))
		if err != nil {
			return err
		}

		if foundKeys, ok := res[1].([]interface{}); ok {
			client.Send("DEL", foundKeys...)

			if len(foundKeys) == 0 {
				break
			}
		}

		cursor = res[0]
	}

	return nil
}

// IncreaseCachedValueByOne will increments the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) IncreaseCachedValueByOne(key string) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	_, err := client.Do("INCR", key)
	return err
}

// SetDefaultTTL :nodoc:
func (k *keeper) SetDefaultTTL(d time.Duration) {
	k.defaultTTL = d
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

// AcquireLock :nodoc:
func (k *keeper) AcquireLock(key string) (*redsync.Mutex, error) {
	r := redsync.New([]redsync.Pool{k.lockConnPool})
	m := r.NewMutex("lock:"+key,
		redsync.SetExpiry(k.lockDuration),
		redsync.SetTries(k.lockTries))

	return m, m.Lock()
}

// DeleteByKeys Delete by multiple keys
func (k *keeper) DeleteByKeys(keys []string) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	redisKeys := []interface{}{}
	for _, key := range keys {
		redisKeys = append(redisKeys, key)
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
	defer client.Close()

	client.Send("MULTI")
	for _, item := range items {
		client.Send("SETEX", item.GetKey(), k.decideCacheTTL(item), item.GetValue())
	}

	_, err := client.Do("EXEC")
	return err
}

// StoreMultiPersist Store multiple items with persistence
func (k *keeper) StoreMultiPersist(items []Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	client.Send("MULTI")
	for _, item := range items {
		client.Send("SET", item.GetKey(), item.GetValue())
		client.Send("PERSIST", item.GetKey())
	}

	_, err := client.Do("EXEC")
	return err
}

// Expire Set expire a key
func (k *keeper) Expire(key string, duration time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	_, err = client.Do("EXPIRE", key, int64(duration.Seconds()))
	return
}

// ExpireMulti Set expire multiple
func (k *keeper) ExpireMulti(items map[string]time.Duration) error {
	if k.disableCaching {
		return nil
	}

	client := k.connPool.Get()
	defer client.Close()

	client.Send("MULTI")
	for k, duration := range items {
		client.Send("EXPIRE", k, int64(duration.Seconds()))
	}

	_, err := client.Do("EXEC")
	return err
}

func (k *keeper) decideCacheTTL(c Item) (ttl int64) {
	if ttl = c.GetTTLInt64(); ttl > 0 {
		return
	}

	return int64(k.defaultTTL.Seconds())
}

func (k *keeper) getCachedItem(key string) (value interface{}, err error) {
	client := k.connPool.Get()
	defer client.Close()

	return client.Do("GET", key)
}

func (k *keeper) isLocked(key string) bool {
	client := k.lockConnPool.Get()
	defer client.Close()

	reply, err := client.Do("GET", "lock:"+key)
	if err != nil || reply == nil {
		return false
	}

	return true
}

// CheckKeyExist :nodoc:
func (k *keeper) CheckKeyExist(key string) (value bool, err error) {

	client := k.connPool.Get()
	defer client.Close()

	val, err := client.Do("EXISTS", key)

	value = false
	if val.(int64) > 0 {
		value = true
	}

	return
}

// StoreRightList :nodoc:
func (k *keeper) StoreRightList(name string, value interface{}) error {
	client := k.connPool.Get()
	defer client.Close()

	_, err := client.Do("RPUSH", name, value)

	return err
}

// StoreLeftList :nodoc:
func (k *keeper) StoreLeftList(name string, value interface{}) error {
	client := k.connPool.Get()
	defer client.Close()

	_, err := client.Do("LPUSH", name, value)

	return err
}

func (k *keeper) GetListLength(name string) (value int64, err error) {
	client := k.connPool.Get()
	defer client.Close()

	val, err := client.Do("LLEN", name)
	value = val.(int64)

	return
}

func (k *keeper) GetAndRemoveFirstListElement(name string) (value interface{}, err error) {
	client := k.connPool.Get()
	defer client.Close()

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

func (k *keeper) GetAndRemoveLastListElement(name string) (value interface{}, err error) {
	client := k.connPool.Get()
	defer client.Close()

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

func (k *keeper) GetList(name string, size int64, page int64) (value interface{}, err error) {
	offset := utils.Offset(page, size)

	client := k.connPool.Get()
	defer client.Close()

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
