package cacher

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/jpillora/backoff"
	"github.com/kumparan/redsync/v4"
	redigosync "github.com/kumparan/redsync/v4/redis/redigo"
)

const (
	// Override these when constructing the cache keeper
	defaultTTL          = 10 * time.Second
	defaultNilTTL       = 5 * time.Minute
	defaultLockDuration = 1 * time.Minute
	defaultLockTries    = 1
	defaultWaitTime     = 15 * time.Second
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
		GetMultipleOrLock(keys []string) ([]any, []*redsync.Mutex, error)
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
		connPool       *redigo.Pool
		nilTTL         time.Duration
		defaultTTL     time.Duration
		waitTime       time.Duration
		disableCaching bool

		lockConnPool *redigo.Pool
		lockDuration time.Duration
		lockTries    int
	}

	itemWithKey struct {
		Key  string
		Item any
	}

	mutexWithKey struct {
		Key   string
		Mutex *redsync.Mutex
	}

	errorWithKey struct {
		key        string
		innerError error
	}
)

// Error implement built-in error interface
func (ewk *errorWithKey) Error() string {
	var msg string
	if ewk.innerError != nil {
		msg = ewk.innerError.Error()
	}
	return fmt.Sprintf("err on key %s : %s", ewk.key, msg)
}

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

// Get :nodoc:
func (k *keeper) Get(key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = get(k.connPool.Get(), key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// GetOrLock :nodoc:
func (k *keeper) GetOrLock(key string) (cachedItem any, mutex *redsync.Mutex, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = get(k.connPool.Get(), key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
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
			cachedItem, err = get(k.connPool.Get(), key)
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

// GetMultipleOrLock get multiple and apply locks for non-existing keys on redis.
// Returned cached items will be in order based on keys provided, if the value for some key is not exist then it will be marked as nil on
// returned cached items slice.
func (k *keeper) GetMultipleOrLock(keys []string) (cachedItems []any, mutexes []*redsync.Mutex, err error) {
	if k.disableCaching {
		return
	}

	c := k.connPool.Get()
	defer func() {
		_ = c.Close()
	}()

	err = sendMultipleGetCommands(c, keys)
	if err != nil {
		return
	}

	err = c.Flush()
	if err != nil {
		return
	}

	var (
		keysToLock     []string
		cachedItemsBuf = make(map[string]any)
		mutexesBuf     = make(map[string]*redsync.Mutex)
	)
	for _, k := range keys {
		rep, err := redigo.Bytes(c.Receive())
		if err != nil && err != redigo.ErrNil {
			return nil, nil, err
		}
		if rep == nil {
			keysToLock = append(keysToLock, k)
			continue
		}
		cachedItemsBuf[k] = rep
	}

	var (
		itemCh  = make(chan itemWithKey)
		errCh   = make(chan error)
		mutexCh = make(chan mutexWithKey)
	)

	for _, key := range keysToLock {
		go k.acquireLockOrGetValueThroughChan(key, mutexCh, itemCh, errCh)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var errs *multierror.Error
		counter := 0
		for {
			select {
			case i := <-itemCh:
				cachedItemsBuf[i.Key] = i.Item
				counter++
			case caseErr := <-errCh:
				err = multierror.Append(errs, caseErr)
				counter++
			case m := <-mutexCh:
				mutexesBuf[m.Key] = m.Mutex
				counter++
			default:
				if counter == len(keysToLock) {
					err = errs.ErrorOrNil()
					return
				}
			}
		}
	}()
	wg.Wait()

	for _, k := range keys {
		cachedItems = append(cachedItems, cachedItemsBuf[k])
		if m, ok := mutexesBuf[k]; ok {
			mutexes = append(mutexes, m)
		}
	}

	return
}

func (k *keeper) acquireLockOrGetValueThroughChan(key string, mutexCh chan<- mutexWithKey, itemCh chan<- itemWithKey, errCh chan<- error) {
	mutex, err := k.AcquireLock(key)
	if err == nil {
		mutexCh <- mutexWithKey{Mutex: mutex, Key: key}
		return
	}
	start := time.Now()
	for {
		b := &backoff.Backoff{
			Jitter: true,
			Min:    20 * time.Millisecond,
			Max:    200 * time.Millisecond,
		}

		if !k.isLocked(key) {
			cachedItem, err := get(k.connPool.Get(), key)
			if err != nil {
				if err == ErrKeyNotExist {
					mutex, err = k.AcquireLock(key)
					if err == nil {
						mutexCh <- mutexWithKey{Mutex: mutex, Key: key}
						return
					}
					goto Wait
				}
				errCh <- &errorWithKey{key: key, innerError: err}
				return
			}
			itemCh <- itemWithKey{Item: cachedItem, Key: key}
			return
		}

	Wait:
		elapsed := time.Since(start)
		if elapsed >= k.waitTime {
			errCh <- &errorWithKey{key: key, innerError: ErrWaitTooLong}
			return
		}
		time.Sleep(b.Duration())
	}
}

// Store :nodoc:
func (k *keeper) Store(mutex *redsync.Mutex, c Item) error {
	if k.disableCaching {
		return nil
	}
	defer SafeUnlock(mutex)

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err := client.Do("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
	return err
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

	_, err := client.Do("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
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

// IncreaseCachedValueByOne will increments the number stored at key by one.
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

func sendMultipleGetCommands(c redigo.Conn, keys []string) (err error) {
	for _, key := range keys {
		err = c.Send("GET", key)
		if err != nil {
			return
		}
	}
	return nil
}
