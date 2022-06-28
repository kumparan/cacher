package cacher

import (
	"time"

	"github.com/go-redsync/redsync/v4"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	defaultFailoverTTL = 2 * time.Hour
)

// KeeperWithFailover cache keeper with failover feature
type KeeperWithFailover struct {
	keeper
	failoverConnPool *redigo.Pool
	failoverTTL      time.Duration
}

// NewKeeperWithFailover :nodoc:
func NewKeeperWithFailover() *KeeperWithFailover {
	return &KeeperWithFailover{
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
func (k *KeeperWithFailover) SetFailoverTTL(d time.Duration) {
	k.failoverTTL = d
}

// SetFailoverConnectionPool :nodoc:
func (k *KeeperWithFailover) SetFailoverConnectionPool(c *redigo.Pool) {
	k.failoverConnPool = c
}

// GetOrSet :nodoc:
func (k *KeeperWithFailover) GetOrSet(key string, fn GetterFn, ttl time.Duration) (cachedItem any, err error) {
	cachedItem, mu, err := k.GetOrLock(key)
	if err != nil {
		return
	}
	if cachedItem != nil {
		return
	}

	// handle if nil value is cached
	if mu == nil {
		return
	}
	defer SafeUnlock(mu)

	cachedItem, err = fn()
	if err != nil {
		return k.GetFailover(key)
	}

	if cachedItem == nil {
		err = k.StoreNil(key)
		return
	}

	err = k.Store(mu, NewItemWithCustomTTL(key, cachedItem, ttl))
	err = k.StoreFailover(mu, NewItemWithCustomTTL(key, cachedItem, k.failoverTTL))

	return
}

// GetFailover :nodoc:
func (k *KeeperWithFailover) GetFailover(key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = getCachedItem(k.failoverConnPool.Get(), key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// StoreFailover :nodoc:
func (k *KeeperWithFailover) StoreFailover(mutex *redsync.Mutex, c Item) error {
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
