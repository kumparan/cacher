package cacher

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/bsm/redislock"
	"github.com/jpillora/backoff"
	goredis "github.com/redis/go-redis/v9"
)

// ErrNotFound :nodoc:
var ErrNotFound = errors.New("not found")

const (
	// Override these when constructing the cache keeper
	defaultTTL          = 10 * time.Second
	defaultNilTTL       = 5 * time.Minute
	defaultLockDuration = 1 * time.Minute
	defaultLockTries    = 1
	defaultWaitTime     = 15 * time.Second
)

var nilJSON = []byte("null")

// HashMember :nodoc:
type HashMember struct {
	Identifier string
	Key        string
}

type (
	// RedisConnector :Cluster or Client:
	RedisConnector interface {
		TxPipeline() goredis.Pipeliner
		Pipeline() goredis.Pipeliner

		Get(ctx context.Context, key string) *goredis.StringCmd
		Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
		Incr(ctx context.Context, key string) *goredis.IntCmd
		SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
		Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd
		EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *goredis.Cmd
		EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd
		EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *goredis.Cmd
		ScriptExists(ctx context.Context, hashes ...string) *goredis.BoolSliceCmd
		ScriptLoad(ctx context.Context, script string) *goredis.StringCmd
		Del(ctx context.Context, keys ...string) *goredis.IntCmd
		HDel(ctx context.Context, key string, fields ...string) *goredis.IntCmd
		HGet(ctx context.Context, key, field string) *goredis.StringCmd
		HSet(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
		TTL(ctx context.Context, key string) *goredis.DurationCmd
		RPush(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
		RPop(ctx context.Context, key string) *goredis.StringCmd
		LLen(ctx context.Context, key string) *goredis.IntCmd
		LPop(ctx context.Context, key string) *goredis.StringCmd
		LPush(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
		LRange(ctx context.Context, key string, start, stop int64) *goredis.StringSliceCmd
		Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd
		Exists(ctx context.Context, keys ...string) *goredis.IntCmd
		Do(ctx context.Context, args ...interface{}) *goredis.Cmd
		Persist(ctx context.Context, key string) *goredis.BoolCmd
	}

	// CacheGeneratorFn :nodoc:
	CacheGeneratorFn func() (interface{}, error)

	// Keeper responsible for managing cache
	Keeper interface {
		Get(context.Context, string) (interface{}, error)
		GetOrLock(context.Context, string) (interface{}, *redislock.Lock, error)
		GetOrSet(context.Context, string, CacheGeneratorFn, time.Duration) (interface{}, error)
		Store(context.Context, *redislock.Lock, Item) error
		StoreWithoutBlocking(context.Context, Item) error
		StoreMultiWithoutBlocking(context.Context, []Item) error
		StoreMultiPersist(context.Context, []Item) error
		StoreNil(ctx context.Context, cacheKey string) error
		Expire(context.Context, string, time.Duration) error
		ExpireMulti(context.Context, map[string]time.Duration) error
		// Purge(string) error
		DeleteByKeys(context.Context, []string) error
		IncreaseCachedValueByOne(ctx context.Context, key string) error

		AcquireLock(context.Context, string) (*redislock.Lock, error)
		SetDefaultTTL(time.Duration)
		SetNilTTL(time.Duration)
		SetConnectionPool(RedisConnector)
		SetLockConnectionPool(RedisConnector)
		SetLockDuration(time.Duration)
		SetLockTries(int)
		SetWaitTime(time.Duration)
		SetDisableCaching(bool)

		CheckKeyExist(context.Context, string) (bool, error)

		//list
		StoreRightList(context.Context, string, interface{}) error
		StoreLeftList(context.Context, string, interface{}) error
		GetList(context.Context, string, int64, int64) (interface{}, error)
		GetListLength(context.Context, string) (int64, error)
		GetAndRemoveFirstListElement(context.Context, string) (interface{}, error)
		GetAndRemoveLastListElement(context.Context, string) (interface{}, error)

		GetTTL(context.Context, string) (int64, error)

		// HASH BUCKET
		GetHashMemberOrLock(ctx context.Context, identifier string, key string) (interface{}, *redislock.Lock, error)
		StoreHashMember(context.Context, string, Item) error
		StoreMultiHashMembers(ctx context.Context, mapIdentifiersToItems map[string][]Item) error
		GetHashMember(ctx context.Context, identifier string, key string) (interface{}, error)
		DeleteHashMember(ctx context.Context, identifier string, key string) error
		GetMultiHashMembers(ctx context.Context, hashMember []HashMember) ([]interface{}, error)

		// Persist
		Persist(ctx context.Context, key string) error
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
func (k *keeper) Get(ctx context.Context, key string) (cachedItem interface{}, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(ctx, key)
	if err != nil || cachedItem != nil {
		return
	}

	return nil, nil
}

// GetOrLock :nodoc:
func (k *keeper) GetOrLock(ctx context.Context, key string) (cachedItem interface{}, mutex *redislock.Lock, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(ctx, key)
	if err != nil || cachedItem != nil {
		return
	}

	mutex, err = k.AcquireLock(ctx, key)
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

		if !k.isLocked(ctx, key) {
			cachedItem, err = k.getCachedItem(ctx, key)
			switch {
			// redis error, giving up
			case err != nil && err != goredis.Nil:
				return nil, nil, err
			// cache not found, try to get another lock
			case err == goredis.Nil || cachedItem == nil:
				mutex, err = k.AcquireLock(ctx, key)
				if err == nil {
					return nil, mutex, nil
				}
				// can't acquire lock, let's keep waiting
			// cache found, return it
			default:
				return cachedItem, nil, nil
			}
		}

		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, ErrWaitTooLong
}

// GetOrSet :nodoc:
func (k *keeper) GetOrSet(ctx context.Context, key string, fn CacheGeneratorFn, ttl time.Duration) (cachedItem interface{}, err error) {
	cachedItem, mu, err := k.GetOrLock(ctx, key)
	if err != nil {
		return
	}
	if cachedItem != nil {
		return
	}

	defer SafeUnlock(ctx, mu)

	cachedItem, err = fn()

	if err != nil {
		return
	}

	err = k.Store(ctx, mu, NewItemWithCustomTTL(key, cachedItem, ttl))

	return
}

// Store :nodoc:
func (k *keeper) Store(ctx context.Context, mutex *redislock.Lock, c Item) error {
	if k.disableCaching {
		return nil
	}

	SafeUnlock(ctx, mutex)

	err := k.connPool.Set(ctx, c.GetKey(), c.GetValue(), k.decideCacheTTL(c)).Err()
	return err
}

// StoreWithoutBlocking :nodoc:
func (k *keeper) StoreWithoutBlocking(ctx context.Context, c Item) error {
	if k.disableCaching {
		return nil
	}

	return k.connPool.Set(ctx, c.GetKey(), c.GetValue(), k.decideCacheTTL(c)).Err()
}

// StoreNil :nodoc:
func (k *keeper) StoreNil(ctx context.Context, cacheKey string) error {
	item := NewItemWithCustomTTL(cacheKey, nilJSON, k.nilTTL)
	err := k.StoreWithoutBlocking(ctx, item)
	return err
}

// IncreaseCachedValueByOne will increments the number stored at key by one.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) IncreaseCachedValueByOne(ctx context.Context, key string) error {
	if k.disableCaching {
		return nil
	}

	_, err := k.connPool.Incr(ctx, key).Result()
	return err
}

// AcquireLock :nodoc:
func (k *keeper) AcquireLock(ctx context.Context, key string) (*redislock.Lock, error) {
	lock, err := redislock.Obtain(ctx, k.lockConnPool, "lock:"+key, k.lockDuration, nil)

	return lock, err
}

// DeleteByKeys Delete by multiple keys
func (k *keeper) DeleteByKeys(ctx context.Context, keys []string) error {
	if k.disableCaching {
		return nil
	}

	var redisKeys []string
	for _, key := range keys {
		redisKeys = append(redisKeys, key)
	}

	_, err := k.connPool.Del(ctx, redisKeys...).Result()
	return err
}

// StoreMultiWithoutBlocking Store multiple items
func (k *keeper) StoreMultiWithoutBlocking(ctx context.Context, items []Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for _, item := range items {
		err = pipeline.Set(ctx, item.GetKey(), item.GetValue(), k.decideCacheTTL(item)).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec(ctx)
	return err
}

// StoreMultiPersist Store multiple items with persistence
func (k *keeper) StoreMultiPersist(ctx context.Context, items []Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for _, item := range items {
		err = pipeline.Set(ctx, item.GetKey(), item.GetValue(), 0).Err()
		if err != nil {
			return err
		}
		err = pipeline.Persist(ctx, item.GetKey()).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec(ctx)
	return err
}

// Expire Set expire a key
func (k *keeper) Expire(ctx context.Context, key string, duration time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	err = k.connPool.Expire(ctx, key, duration).Err()
	return
}

// ExpireMulti Set expire multiple
func (k *keeper) ExpireMulti(ctx context.Context, items map[string]time.Duration) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for k, duration := range items {
		err = pipeline.Expire(ctx, k, duration).Err()
		if err != nil {
			return err
		}
	}

	_, err = pipeline.Exec(ctx)
	return err
}

func (k *keeper) decideCacheTTL(c Item) (ttl time.Duration) {
	if intTTL := c.GetTTLInt64(); intTTL > 0 {
		ttl = time.Duration(intTTL) * time.Second
		return
	}

	return k.defaultTTL
}

func (k *keeper) getCachedItem(ctx context.Context, key string) (value interface{}, err error) {
	if k.disableCaching {
		return
	}

	resp, err := k.connPool.Get(ctx, key).Result()
	switch {
	case err != nil && err != goredis.Nil:
		value = nil
		return
	case err == goredis.Nil:
		value = nil
		err = nil
		return
	case resp == "":
		value = nil
		return
	}
	value = StringToBytes(resp)

	return
}

func (k *keeper) isLocked(ctx context.Context, key string) bool {
	reply, err := k.lockConnPool.Exists(ctx, "lock:"+key).Result()
	if err != nil || reply == 0 {
		return false
	}

	return true
}

// CheckKeyExist :nodoc:
func (k *keeper) CheckKeyExist(ctx context.Context, key string) (value bool, err error) {
	val, err := k.connPool.Exists(ctx, key).Result()

	value = false
	if val > 0 {
		value = true
	}

	return
}

// StoreRightList :nodoc:
func (k *keeper) StoreRightList(ctx context.Context, name string, value interface{}) error {
	return k.connPool.RPush(ctx, name, value).Err()
}

// StoreLeftList :nodoc:
func (k *keeper) StoreLeftList(ctx context.Context, name string, value interface{}) error {
	return k.connPool.LPush(ctx, name, value).Err()
}

func (k *keeper) GetListLength(ctx context.Context, name string) (value int64, err error) {
	return k.connPool.LLen(ctx, name).Result()
}

func (k *keeper) GetAndRemoveFirstListElement(ctx context.Context, name string) (value interface{}, err error) {
	llen, err := k.GetListLength(ctx, name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = k.connPool.LPop(ctx, name).Result()
	return
}

func (k *keeper) GetAndRemoveLastListElement(ctx context.Context, name string) (value interface{}, err error) {
	llen, err := k.GetListLength(ctx, name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	value, err = k.connPool.RPop(ctx, name).Result()
	return
}

func (k *keeper) GetList(ctx context.Context, name string, size int64, page int64) (value interface{}, err error) {
	offset := getOffset(page, size)

	llen, err := k.GetListLength(ctx, name)
	if err != nil {
		return
	}

	if llen == 0 {
		return
	}

	end := offset + size

	resp, err := k.connPool.LRange(ctx, name, offset, end).Result()
	if len(resp) == 0 {
		value = nil
		return
	}
	value = resp
	return
}

func (k *keeper) GetTTL(ctx context.Context, name string) (value int64, err error) {
	val, err := k.connPool.TTL(ctx, name).Result()
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
func (k *keeper) StoreHashMember(ctx context.Context, identifier string, c Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	err = pipeline.HSet(ctx, identifier, c.GetKey(), c.GetValue()).Err()
	if err != nil {
		return err
	}
	err = pipeline.Expire(ctx, identifier, k.decideCacheTTL(c)).Err()
	if err != nil {
		return err
	}

	_, err = pipeline.Exec(ctx)
	return
}

// StoreMultiHashMembers :nodoc:
func (k *keeper) StoreMultiHashMembers(ctx context.Context, mapIdentifiersToMembers map[string][]Item) (err error) {
	if k.disableCaching {
		return nil
	}

	pipeline := k.connPool.TxPipeline()
	for i, items := range mapIdentifiersToMembers {
		for _, v := range items {
			err = pipeline.HSet(ctx, i, v.GetKey(), v.GetValue()).Err()
			if err != nil {
				return err
			}
			err = pipeline.Expire(ctx, i, k.decideCacheTTL(v)).Err()
			if err != nil {
				return err
			}
		}
	}

	_, err = pipeline.Exec(ctx)
	return
}

// GetHashMemberOrLock :nodoc:
func (k *keeper) GetHashMemberOrLock(ctx context.Context, identifier string, key string) (cachedItem interface{}, mutex *redislock.Lock, err error) {
	if k.disableCaching {
		return
	}

	lockKey := fmt.Sprintf("%s:%s", identifier, key)

	cachedItem, err = k.GetHashMember(ctx, identifier, key)
	if err != nil || cachedItem != nil {
		return
	}

	mutex, err = k.AcquireLock(ctx, lockKey)
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

		if !k.isLocked(ctx, lockKey) {
			cachedItem, err = k.GetHashMember(ctx, identifier, key)
			switch {
			// redis error, giving up
			case err != nil && err != goredis.Nil:
				return nil, nil, err
			// cache not found, try to get another lock
			case err == goredis.Nil || cachedItem == nil:
				mutex, err = k.AcquireLock(ctx, lockKey)
				if err == nil {
					return nil, mutex, nil
				}
				// can't acquire lock, let's keep waiting
			// cache found, return it
			default:
				return cachedItem, nil, nil
			}
		}

		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			break
		}

		time.Sleep(b.Duration())
	}

	return nil, nil, ErrWaitTooLong
}

// GetHashMember :nodoc:
func (k *keeper) GetHashMember(ctx context.Context, identifier string, key string) (value interface{}, err error) {
	if k.disableCaching {
		return
	}
	resp, err := k.connPool.HGet(ctx, identifier, key).Result()
	switch {
	case err != nil && err != goredis.Nil:
		value = nil
		return
	case err == goredis.Nil:
		value = nil
		err = nil
		return
	case resp == "":
		value = nil
		return
	}
	value = StringToBytes(resp)
	return
}

// GetMultiHashMembers return type is *goredis.StringCmd
// to reduce looping and casting, so the caller is the one that should cast it
func (k *keeper) GetMultiHashMembers(ctx context.Context, hashMember []HashMember) (replies []interface{}, err error) {
	if len(hashMember) == 0 {
		return nil, nil
	}
	if k.disableCaching {
		var replies []interface{}
		for range hashMember {
			replies = append(replies, goredis.NewStringResult("", goredis.Nil))
		}
		return replies, nil
	}

	pipe := k.connPool.Pipeline()
	for _, hm := range hashMember {
		replies = append(replies, pipe.HGet(ctx, hm.Identifier, hm.Key))
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("failed to exec pipe: %w", err)
	}

	return replies, nil
}

// DeleteHashMember :nodoc:
func (k *keeper) DeleteHashMember(ctx context.Context, identifier string, key string) (err error) {
	if k.disableCaching {
		return
	}

	return k.connPool.HDel(ctx, identifier, key).Err()
}

// Persist :nodoc:
func (k *keeper) Persist(ctx context.Context, key string) (err error) {
	if k.disableCaching {
		return
	}

	return k.connPool.Persist(ctx, key).Err()
}

// GetGoredisResult :nodoc:
func GetGoredisResult(reply interface{}) (string, error) {
	strCMD, ok := reply.(*goredis.StringCmd)
	if !ok {
		return "", errors.New("")
	}

	res, err := strCMD.Result()
	if errors.Is(err, goredis.Nil) {
		return "", ErrNotFound
	}

	return res, nil
}

func SafeUnlock(ctx context.Context, mutex *redislock.Lock) {
	if mutex != nil {
		_ = mutex.Release(ctx)
	}
}

func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
