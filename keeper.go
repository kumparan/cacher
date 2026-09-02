package cacher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hashicorp/go-multierror"

	"github.com/sirupsen/logrus"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/jpillora/backoff"
	"github.com/kumparan/go-utils"
	"github.com/kumparan/redsync/v4"
	redigosync "github.com/kumparan/redsync/v4/redis/redigo"
)

const (
	// Override these when constructing the cache keeper
	defaultTTL                              = 10 * time.Second
	defaultNilTTL                           = 5 * time.Minute
	defaultLockDuration                     = 5 * time.Second
	defaultWaitTime                         = 5 * time.Second
	defaultMaxCacheTTL                      = 48 * time.Hour
	defaultMinCacheTTLThreshold             = 5 * time.Second
	defaultLockTries                        = 1
	defaultCacheHitThreshold                = 10
	defaultMultiplierFactor                 = 2
	defaultBackoffMinDurationForLockAttempt = 20 * time.Millisecond
	defaultBackoffMaxDurationForLockAttempt = 200 * time.Millisecond
	defaultRedisPoolMetricsLoggerInterval   = 10 * time.Second
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
		GetMultiple(keys []string) (map[string]any, error)
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
		IncreaseCachedValueByOne(key string, ttl time.Duration) (int64, error)

		IncreaseValueBy(key string, increaseBy int64, ttl time.Duration) (int64, error)
		DecreaseValueBy(key string, decreaseBy int64, ttl time.Duration) (int64, error)

		AcquireLock(string) (*redsync.Mutex, error)
		SetDefaultTTL(time.Duration)
		SetNilTTL(time.Duration)
		SetConnectionPool(*redigo.Pool)
		SetLockConnectionPool(*redigo.Pool)
		SetLockDuration(time.Duration)
		SetLockTries(int)
		SetWaitTime(time.Duration)
		SetDisableCaching(bool)
		IsCachingDisabled() bool
		SetEnableDynamicTTL(bool)
		SetMaxCacheTTL(time.Duration)
		SetMinCacheTTLThreshold(time.Duration)
		SetCacheHitThreshold(int64)
		SetMultiplierFactor(int64)

		CheckKeyExist(string) (bool, error)

		// list
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
		GetMultiHashMembersOrLock(identifiers []string, keys []string) (cachedItems []any, mutexes []*redsync.Mutex, err error)
		StoreHashNilMember(identifier, cacheKey string) error
		GetHashMember(identifier string, key string) (any, error)
		DeleteHashMember(identifier string, key string) error
		IncreaseHashMemberValue(identifier, key string, value int64) (int64, error)
		GetHashMemberThenDelete(identifier, key string) (any, error)
		HashScan(identifier string, cursor int64) (next int64, result map[string]string, err error)

		StartPoolMetricsLogger(ctx context.Context, interval time.Duration)
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

	itemWithKey struct {
		Key  string
		Item any
	}

	itemWithKeyAndIdentifier struct {
		Key        string
		Item       any
		Identifier string
	}

	mutexWithKey struct {
		Key   string
		Mutex *redsync.Mutex
	}

	mutexWithKeyAndIdentifier struct {
		Key        string
		Mutex      *redsync.Mutex
		Identifier string
	}

	errorWithKey struct {
		key        string
		innerError error
	}

	// KeyIdentifier pairs a cache key with the identifier its loader needs to fetch it.
	KeyIdentifier[T any] struct {
		Key        string
		Identifier T
	}

	redisPoolSnapshot struct {
		waitCount    int64
		waitDuration time.Duration
	}

	keyTTL struct {
		key string
		ttl int64
	}

	lockResult struct {
		mutex *redsync.Mutex
		err   error
	}
)

// Error implements built-in error interface
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

// IsCachingDisabled returns whether caching is disabled or not
func (k *keeper) IsCachingDisabled() bool {
	return k.disableCaching
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

	conn := k.connPool.Get()
	cachedItem, ttl, err := get(conn, key)
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
		return // nolint:nilerr
	}

	start := time.Now()
	for {
		b := &backoff.Backoff{
			Min:    defaultBackoffMinDurationForLockAttempt,
			Max:    defaultBackoffMaxDurationForLockAttempt,
			Jitter: true,
		}

		if !k.isLocked(key) {
			conn := k.connPool.Get()
			cachedItem, ttlValue, err := get(conn, key)
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
	if k.disableCaching {
		myResp, err := fn()
		if err != nil {
			return nil, err
		}

		return json.Marshal(myResp)
	}

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

// GetMultipleOrLock DEPRECATED because of deadlock issue, use GetMultipleOrLoad
// get multiple and apply locks for non-existing keys on redis.
// Returned cached items will be in order based on keys provided, if the value for some key is not exist then it will be marked as nil on
// returned cached items slice.
func (k *keeper) GetMultipleOrLock(keys []string) (cachedItems []any, mutexes []*redsync.Mutex, err error) {
	if k.disableCaching {
		for range keys {
			cachedItems = append(cachedItems, nil)
		}
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
	for _, key := range keys {
		rep, err := redigo.Bytes(c.Receive())
		if err != nil && err != redigo.ErrNil {
			return nil, nil, err
		}
		if rep == nil {
			keysToLock = append(keysToLock, key)
			_, _ = c.Receive()
			continue
		}
		ttl, err := c.Receive()
		if err != nil {
			return nil, nil, err
		}

		cachedItemsBuf[key] = rep
		if k.enableDynamicTTL {
			k.extendCacheTTL(key, ttl.(int64))
		}
	}

	var (
		itemCh  = make(chan itemWithKey)
		errCh   = make(chan error)
		mutexCh = make(chan mutexWithKey)
	)
	keysToLock = utils.Unique(keysToLock)
	for _, key := range keysToLock {
		go k.acquireLockOrGetValueThroughChan(key, mutexCh, itemCh, errCh)
	}

	var errs *multierror.Error
	counter := 0
	for counter < len(keysToLock) {
		select {
		case i := <-itemCh:
			cachedItemsBuf[i.Key] = i.Item
		case caseErr := <-errCh:
			errs = multierror.Append(errs, caseErr)
		case m := <-mutexCh:
			mutexesBuf[m.Key] = m.Mutex
		}
		counter++
	}
	err = errs.ErrorOrNil()
	for _, k := range keys {
		cachedItems = append(cachedItems, cachedItemsBuf[k])
		if m, ok := mutexesBuf[k]; ok {
			mutexes = append(mutexes, m)
		}
	}

	return
}

// GetMultiple returns a map of cached values for the given keys. If a key does not exist in the cache, it will not be included in the returned map. The function also handles dynamic TTL extension if enabled.
func (k *keeper) GetMultiple(keys []string) (map[string]any, error) {
	if k.disableCaching || len(keys) == 0 {
		return map[string]any{}, nil
	}

	uniqueKeys := utils.Unique(keys)
	result := make(map[string]any, len(uniqueKeys))
	ttls := make([]keyTTL, 0, len(uniqueKeys))

	err := func() error {
		conn := k.connPool.Get()
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				logrus.Error(closeErr)
			}
		}()

		for _, key := range uniqueKeys {
			if err := conn.Send("GET", key); err != nil {
				return err
			}
			if k.enableDynamicTTL {
				if err := conn.Send("TTL", key); err != nil {
					return err
				}
			}
		}
		if err := conn.Flush(); err != nil {
			return err
		}

		for _, key := range uniqueKeys {
			value, err := redigo.Bytes(conn.Receive())
			if err != nil && !errors.Is(err, redigo.ErrNil) {
				return err
			}

			if k.enableDynamicTTL {
				ttl, ttlErr := redigo.Int64(conn.Receive())
				if ttlErr != nil {
					return ttlErr
				}
				if value != nil {
					ttls = append(ttls, keyTTL{key: key, ttl: ttl})
				}
			}

			if value != nil {
				result[key] = value
			}
		}

		return nil
	}()

	if err != nil {
		return nil, err
	}

	k.applyDynamicTTLPolicy(ttls)

	return result, nil
}

// GetMultipleOrLoad returns the cached value for every item, in the same order as items.
// Keys that miss are locked and loaded via loader; if another process already holds the
// lock, this waits for that process to fill the cache instead of loading twice.
//
// loader receives the identifiers of the keys it must load, and must return a map keyed
// by cache key holding already-serialized values. Keys absent from that map are cached
// as JSON null.
//
// ctx should carry a deadline: waiting on another process's lock has no other bound.
func GetMultipleOrLoad[T any](
	ctx context.Context,
	k Keeper,
	items []KeyIdentifier[T],
	loader func(ctx context.Context, identifiers []T) (map[string][]byte, error),
) ([]any, error) {
	identifierByKey := make(map[string]T, len(items))
	pending := make([]string, 0, len(items))
	for _, it := range items {
		if _, seen := identifierByKey[it.Key]; seen {
			continue
		}
		identifierByKey[it.Key] = it.Identifier
		pending = append(pending, it.Key)
	}

	// call the loader directly if caching is disabled
	if k.IsCachingDisabled() {
		values, err := loader(ctx, utils.MapValuesToOrderedSlice(identifierByKey, pending))
		if err != nil {
			return nil, err
		}
		res := make([]any, len(items))
		for i, it := range items {
			if v, found := values[it.Key]; found {
				res[i] = v
			} else {
				res[i] = nilValue
			}
		}
		return res, nil
	}

	result := make(map[string]any, len(pending))
	bo := &backoff.Backoff{
		Min:    defaultBackoffMinDurationForLockAttempt,
		Max:    defaultBackoffMaxDurationForLockAttempt,
		Jitter: true,
	}
	loaderCallCount := 0
	defer func() {
		if loaderCallCount > 1 {
			logrus.Warn("loader called more than once:", loaderCallCount)
		}
	}()
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// check/recheck cache
		missing := make([]string, 0, len(pending))
		cached, err := k.GetMultiple(pending)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}

		for _, key := range pending {
			if value, exists := cached[key]; exists {
				result[key] = value
				continue
			}

			missing = append(missing, key)
		}

		if len(missing) == 0 {
			break
		}

		// lock what we can; leave the rest to whoever holds the lock
		mutexes, locked, waiting, err := acquireLocksConcurrently(k, missing)
		if err != nil {
			logrus.WithError(err).Error("failed to lock one/more keys")
		}
		if len(locked) > 0 {
			loaderCallCount++
			values, err := loader(ctx, utils.MapValuesToOrderedSlice(identifierByKey, locked))
			if err != nil {
				SafeUnlock(mutexes...)
				return nil, err
			}

			cacheItems := make([]Item, 0, len(locked))
			for _, key := range locked {
				value, found := values[key]
				if !found {
					value = nilValue
				}
				cacheItems = append(cacheItems, NewItem(key, value))
				result[key] = value
			}
			if err := k.StoreMultiWithoutBlocking(cacheItems); err != nil {
				logrus.Error(err)
			}
			SafeUnlock(mutexes...)
		}

		pending = waiting
		if len(pending) == 0 {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(bo.Duration()):
		}
	}

	res := make([]any, len(items))
	for i, it := range items {
		res[i] = result[it.Key]
	}
	return res, nil
}

func acquireLocksConcurrently(k Keeper, keysToLock []string) (mutexes []*redsync.Mutex, lockedKeys []string, waitingKeys []string, err error) {
	var g errgroup.Group
	g.SetLimit(lockConcurrencyLimit)

	results := make([]lockResult, len(keysToLock))
	for i, key := range keysToLock {
		key := key
		g.Go(func() error {
			mutex, lockErr := k.AcquireLock(key)
			results[i] = lockResult{mutex: mutex, err: lockErr}
			return nil
		})
	}

	_ = g.Wait()

	mutexes = make([]*redsync.Mutex, 0, len(keysToLock))
	lockedKeys = make([]string, 0, len(keysToLock))
	waitingKeys = make([]string, 0, len(keysToLock))

	var errs *multierror.Error
	for i, key := range keysToLock {
		res := results[i]
		if res.err != nil {
			if !errors.Is(res.err, redsync.ErrFailed) {
				errs = multierror.Append(errs, fmt.Errorf("acquire lock for %q: %w", key, res.err))
			}
			waitingKeys = append(waitingKeys, key)
			continue
		}

		mutexes = append(mutexes, res.mutex)
		lockedKeys = append(lockedKeys, key)
	}

	return mutexes, lockedKeys, waitingKeys, errs.ErrorOrNil()
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
			Min:    defaultBackoffMinDurationForLockAttempt,
			Max:    defaultBackoffMaxDurationForLockAttempt,
		}

		if !k.isLocked(key) {
			cachedItem, err := k.Get(key)
			switch {
			case err != nil:
				errCh <- &errorWithKey{key: key, innerError: err}
				return
			case cachedItem == nil:
				mutex, err = k.AcquireLock(key)
				if err == nil {
					mutexCh <- mutexWithKey{Mutex: mutex, Key: key}
					return
				}
				goto Wait
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

func (k *keeper) acquireLockOrHGetValueThroughChan(identifier string, key string, mutexCh chan<- mutexWithKeyAndIdentifier, itemCh chan<- itemWithKeyAndIdentifier, errCh chan<- error) {
	lockKey := fmt.Sprintf("%s:%s", identifier, key)
	mutex, err := k.AcquireLock(lockKey)
	if err == nil {
		mutexCh <- mutexWithKeyAndIdentifier{Mutex: mutex, Key: key, Identifier: identifier}
		return
	}
	start := time.Now()
	for {
		b := &backoff.Backoff{
			Jitter: true,
			Min:    defaultBackoffMinDurationForLockAttempt,
			Max:    defaultBackoffMaxDurationForLockAttempt,
		}

		if !k.isLocked(key) {
			cachedItem, err := k.GetHashMember(identifier, key)
			if err != nil {
				if err == ErrKeyNotExist {
					mutex, err = k.AcquireLock(lockKey)
					if err == nil {
						mutexCh <- mutexWithKeyAndIdentifier{Mutex: mutex, Key: key, Identifier: identifier}
						return
					}
					goto Wait
				}
				errCh <- &errorWithKey{key: key, innerError: err}
				return
			}
			itemCh <- itemWithKeyAndIdentifier{Identifier: identifier, Key: key, Item: cachedItem}
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
func (k *keeper) IncreaseCachedValueByOne(key string, ttl time.Duration) (int64, error) {
	if k.disableCaching {
		return 0, nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	err = client.Send("INCR", key)
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	if ttl <= 0 {
		ttl = k.defaultTTL
	}

	err = client.Send("EXPIRE", key, ttl.Seconds(), "NX")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	reply, err := client.Do("EXEC")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	replies := reply.([]interface{})

	return replies[0].(int64), err
}

// IncreaseValueBy will increment the value by given integer.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) IncreaseValueBy(key string, incrBy int64, ttl time.Duration) (int64, error) {
	if k.disableCaching {
		return 0, nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	err = client.Send("INCRBY", key, incrBy)
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	if ttl <= 0 {
		ttl = k.defaultTTL
	}

	err = client.Send("EXPIRE", key, ttl.Seconds(), "NX")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	reply, err := client.Do("EXEC")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	replies := reply.([]interface{})

	return replies[0].(int64), err
}

// DecreaseValueBy will decrement the value by given integer.
// If the key does not exist, it is set to 0 before performing the operation
func (k *keeper) DecreaseValueBy(key string, decrBy int64, ttl time.Duration) (int64, error) {
	if k.disableCaching {
		return 0, nil
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err := client.Send("MULTI")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	err = client.Send("DECRBY", key, decrBy)
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	if ttl <= 0 {
		ttl = k.defaultTTL
	}

	err = client.Send("EXPIRE", key, ttl.Seconds(), "NX")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	reply, err := client.Do("EXEC")
	if err != nil {
		logrus.Error(err)
		return 0, err
	}

	replies := reply.([]interface{})

	return replies[0].(int64), err
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
		if k.enableDynamicTTL {
			// set counter cache to 0 with the same TTL as the main cache key
			err = client.Send("SETEX", getCounterKey(item.GetKey()), k.decideCacheTTL(item), 0)
			if err != nil {
				return err
			}
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
		return // nolint:nilerr
	}

	start := time.Now()
	for {
		b := &backoff.Backoff{
			Min:    defaultBackoffMinDurationForLockAttempt,
			Max:    defaultBackoffMaxDurationForLockAttempt,
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

// GetMultiHashMembersOrLock get multiple hash members if the item exists, otherwise it will return the mutexes for non-existing items.
// Returned cachedItems will have the same length as the keys length eventhough the item is not exist. The value of non-existing item will be nil.
// Returned mutexes will have the same length as the non-existing items.
// TODO: refactor this when you are bored
func (k *keeper) GetMultiHashMembersOrLock(identifiers []string, keys []string) (cachedItems []any, mutexes []*redsync.Mutex, err error) {
	if k.disableCaching {
		for range keys {
			cachedItems = append(cachedItems, nil)
		}
		return
	}

	if len(identifiers) != len(keys) {
		return nil, nil, fmt.Errorf("identifiers and keys must have the same length")
	}

	c := k.connPool.Get()
	defer func() {
		_ = c.Close()
	}()

	for i, id := range identifiers {
		err = c.Send("HEXISTS", id, keys[i])
		if err != nil {
			return nil, nil, err
		}
		err = c.Send("HGET", id, keys[i])
		if err != nil {
			return nil, nil, err
		}
	}

	err = c.Flush()
	if err != nil {
		return
	}

	var (
		keyIndexesToLock []int
		cachedItemsBuf   = make(map[string]any)
		mutexesBuf       = make(map[string]*redsync.Mutex)
	)

	for i, id := range identifiers {
		exists, err := redigo.Int(c.Receive())
		if err != nil {
			return nil, nil, err
		}
		if exists == 0 {
			keyIndexesToLock = append(keyIndexesToLock, i)
			_, _ = c.Receive()
			continue
		}

		cachedItem, err := redigo.Bytes(c.Receive())
		if err != nil {
			return nil, nil, err
		}
		cachedItemsBuf[fmt.Sprintf("%s:%s", id, keys[i])] = cachedItem
	}

	var (
		itemCh        = make(chan itemWithKeyAndIdentifier)
		errCh         = make(chan error)
		mutexCh       = make(chan mutexWithKeyAndIdentifier)
		duplicateFlag = make(map[string]bool)
	)

	for _, idx := range keyIndexesToLock {
		if duplicateFlag[fmt.Sprintf("%s:%s", identifiers[idx], keys[idx])] {
			continue
		}
		duplicateFlag[fmt.Sprintf("%s:%s", identifiers[idx], keys[idx])] = true
		go k.acquireLockOrHGetValueThroughChan(identifiers[idx], keys[idx], mutexCh, itemCh, errCh)
	}

	// wait for lock or value
	var errs *multierror.Error
	counter := 0
	for counter < len(keyIndexesToLock) {
		select {
		case i := <-itemCh:
			cachedItemsBuf[fmt.Sprintf("%s:%s", i.Identifier, i.Key)] = i.Item
		case caseErr := <-errCh:
			errs = multierror.Append(errs, caseErr)
		case m := <-mutexCh:
			mutexesBuf[fmt.Sprintf("%s:%s", m.Identifier, m.Key)] = m.Mutex
		}
		counter++
	}
	err = errs.ErrorOrNil()
	for i, id := range identifiers {
		cachedItems = append(cachedItems, cachedItemsBuf[fmt.Sprintf("%s:%s", id, keys[i])])
		if m, ok := mutexesBuf[fmt.Sprintf("%s:%s", id, keys[i])]; ok {
			mutexes = append(mutexes, m)
		}
	}

	return
}

// GetHashMemberOrSet :nodoc:
func (k *keeper) GetHashMemberOrSet(identifier, key string, fn GetterFn, opts ...func(Item)) (res []byte, err error) {
	if k.disableCaching {
		myResp, err := fn()
		if err != nil {
			return nil, err
		}

		return json.Marshal(myResp)
	}
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

// StartPoolMetricsLogger starts a goroutine that logs the connection pool metrics at the specified interval.
func (k *keeper) StartPoolMetricsLogger(
	ctx context.Context,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = defaultRedisPoolMetricsLoggerInterval
	}

	previous := make(map[string]redisPoolSnapshot)

	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				k.logPoolMetrics("cache", k.connPool, previous)
				k.logPoolMetrics("lock", k.lockConnPool, previous)
			}
		}
	}()
}

func (k *keeper) decideCacheTTL(c Item) (ttl int64) {
	if ttl = c.GetTTLInt64(); ttl > 0 {
		return
	}

	return int64(k.defaultTTL.Seconds())
}

// applyDynamicTTLPolicy applies the dynamic TTL policy to a whole batch of
// keys using two pipelined round trips
// Round trip one bumps every hit counter; round trip two extends only the keys
// whose counter crossed the threshold.
func (k *keeper) applyDynamicTTLPolicy(items []keyTTL) {
	if len(items) == 0 || !k.enableDynamicTTL {
		return
	}

	// Filter locally first: keys below the threshold never needed a round trip.
	minTTL := int64(k.minCacheTTLThreshold.Seconds())
	candidates := make([]keyTTL, 0, len(items))
	for _, item := range items {
		if item.ttl >= minTTL {
			candidates = append(candidates, item)
		}
	}
	if len(candidates) == 0 {
		return
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()

	for _, item := range candidates {
		if err := client.Send("INCR", getCounterKey(item.key)); err != nil {
			logrus.Error(err)
			return
		}
	}
	if err := client.Flush(); err != nil {
		logrus.Error(err)
		return
	}

	due := make([]keyTTL, 0, len(candidates))
	for _, item := range candidates {
		counterValue, err := redigo.Int64(client.Receive())
		if err != nil {
			logrus.Error(err)
			return
		}
		if counterValue <= 0 || counterValue%k.cacheHitThreshold != 0 {
			continue
		}
		due = append(due, item)
	}
	if len(due) == 0 {
		return
	}

	for _, item := range due {
		newTTL := item.ttl * k.multiplierFactor
		if newTTL > int64(k.maxCacheTTL) {
			newTTL = int64(k.maxCacheTTL)
		}

		if err := client.Send("EXPIRE", item.key, newTTL, "GT"); err != nil {
			logrus.Error(err)
			return
		}
		if err := client.Send("EXPIRE", getCounterKey(item.key), newTTL, "GT"); err != nil {
			logrus.Error(err)
			return
		}
	}
	if err := client.Flush(); err != nil {
		logrus.Error(err)
		return
	}

	for range due {
		for i := 0; i < 2; i++ {
			if _, err := client.Receive(); err != nil {
				logrus.Error(err)
				return
			}
		}
	}
}

// extendCacheTTL will increase cache TTL based on traffic
// if the traffic reaches the threshold, it will extend the cache TTL
// will not return error as this should not disturb the main operation
func (k *keeper) extendCacheTTL(key string, ttl int64) {
	// if current TTL is below the minimum threshold, just ignore it
	if ttl < int64(k.minCacheTTLThreshold.Seconds()) {
		return
	}

	client := k.connPool.Get()
	defer func() {
		_ = client.Close()
	}()
	counterKey := getCounterKey(key)
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

func (k *keeper) logPoolMetrics(
	poolName string,
	pool *redigo.Pool,
	previous map[string]redisPoolSnapshot,
) {
	if pool == nil {
		return
	}

	stats := pool.Stats()

	prev, exists := previous[poolName]

	waitCountDelta := int64(0)
	waitDurationDelta := time.Duration(0)
	avgWaitDuration := time.Duration(0)

	if exists {
		waitCountDelta = stats.WaitCount - prev.waitCount
		waitDurationDelta = stats.WaitDuration - prev.waitDuration
	}
	if waitCountDelta > 0 {
		avgWaitDuration = waitDurationDelta / time.Duration(waitCountDelta)
	}

	previous[poolName] = redisPoolSnapshot{
		waitCount:    stats.WaitCount,
		waitDuration: stats.WaitDuration,
	}

	logrus.WithFields(logrus.Fields{
		"pool":                   poolName,
		"active_count":           stats.ActiveCount,
		"idle_count":             stats.IdleCount,
		"in_use":                 stats.ActiveCount - stats.IdleCount,
		"wait_count_delta":       waitCountDelta,
		"wait_duration_delta_ms": waitDurationDelta.Milliseconds(),
		"avg_wait_duration_ms":   avgWaitDuration.Milliseconds(),
	}).Info("redis connection pool stats")
}

func sendMultipleGetCommands(c redigo.Conn, keys []string) (err error) {
	for _, key := range keys {
		err = c.Send("GET", key)
		if err != nil {
			return
		}
		err = c.Send("TTL", key)
		if err != nil {
			return
		}
	}
	return nil
}

func getCounterKey(mainKey string) string {
	return fmt.Sprintf("%s:cache:hit:counter", mainKey)
}
