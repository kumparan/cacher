package cacher

import (
	"time"

	"github.com/go-redsync/redsync/v4"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	defaultFailoverTTL = 2 * time.Hour
)

type keeperWithFailover struct {
	keeper
	failoverConnPool *redigo.Pool
	failoverTTL      time.Duration
}

// NewKeeperWithFailover :nodoc:
func NewKeeperWithFailover() *keeperWithFailover {
	return &keeperWithFailover{
		keeper: keeper{
			defaultTTL:     defaultTTL,
			nilTTL:         defaultNilTTL,
			lockDuration:   defaultLockDuration,
			lockTries:      defaultLockTries,
			waitTime:       defaultWaitTime,
			disableCaching: false,
		},
		failoverTTL: defaultFailoverTTL,
	}
}

// SetFailoverTTL :nodoc:
func (k *keeperWithFailover) SetFailoverTTL(d time.Duration) {
	k.failoverTTL = d
}

// SetFailoverConnectionPool :nodoc:
func (k *keeperWithFailover) SetFailoverConnectionPool(c *redigo.Pool) {
	k.failoverConnPool = c
}

// GetOrSet :nodoc:
func (k *keeperWithFailover) GetOrSet(key string, fn GetterFn, ttl time.Duration) (cachedItem any, err error) {
	cachedItem, mu, err := k.GetOrLock(key)
	if err != nil {
		return
	}
	if cachedItem != nil {
		return
	}

	defer SafeUnlock(mu)

	cachedItem, err = fn()
	if err != nil {
		return k.GetFailover(key)
	}

	err = k.Store(mu, NewItemWithCustomTTL(key, cachedItem, ttl))
	err = k.StoreFailover(mu, NewItemWithCustomTTL(key, cachedItem, k.failoverTTL))

	return
}

// GetFailover :nodoc:
func (k *keeperWithFailover) GetFailover(key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = k.getCachedItem(key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// StoreFailover :nodoc:
func (k *keeperWithFailover) StoreFailover(mutex *redsync.Mutex, c Item) error {
	if k.disableCaching {
		return nil
	}
	defer SafeUnlock(mutex)

	client := k.failoverConnPool.Get()
	defer func() {
		_ = client.Close()
	}()

	_, err := client.Do("SETEX", c.GetKey(), k.decideCacheTTL(c), c.GetValue())
	return err
}

func (k *keeperWithFailover) getCachedItem(key string) (value any, err error) {
	client := k.failoverConnPool.Get()
	defer func() {
		_ = client.Close()
	}()

	err = client.Send("MULTI")
	if err != nil {
		return nil, err
	}
	err = client.Send("EXISTS", key)
	if err != nil {
		return nil, err
	}
	err = client.Send("GET", key)
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
