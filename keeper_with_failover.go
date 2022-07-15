package cacher

import (
	"errors"
	"time"

	"github.com/kumparan/tapao"

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
			serializer:     defaultSerializer,
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
func (k *KeeperWithFailover) GetOrSet(key string, fn GetterFn, opts ...func(Item)) (res []byte, err error) {
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
		cachedValue, err = k.GetFailover(key)
		if err != nil {
			return nil, err
		}

		return cachedValue.([]byte), nil
	}

	if item == nil {
		_ = k.StoreNil(key)
		return
	}

	cachedValue, err = tapao.Marshal(item, tapao.With(k.serializer))
	if err != nil {
		return
	}

	cacheItem := NewItem(key, cachedValue)
	for _, o := range opts {
		o(cacheItem)
	}
	_ = k.StoreWithoutBlocking(cacheItem)
	_ = k.StoreFailover(cacheItem)

	return cachedValue.([]byte), nil
}

// GetFailover :nodoc:
func (k *KeeperWithFailover) GetFailover(key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = get(k.failoverConnPool.Get(), key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// StoreFailover :nodoc:
func (k *KeeperWithFailover) StoreFailover(c Item) error {
	if k.disableCaching {
		return nil
	}

	client := k.failoverConnPool.Get()
	defer func() {
		_ = client.Close()
	}()
	_, err := client.Do("SETEX", c.GetKey(), k.failoverTTL.Seconds(), c.GetValue())
	return err
}

// GetHashMemberOrSet :nodoc:
func (k *KeeperWithFailover) GetHashMemberOrSet(identifier, key string, fn GetterFn, opts ...func(Item)) (res []byte, err error) {
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
		cachedValue, err = k.GetHashMemberFailover(identifier, key)
		if err != nil {
			return nil, err
		}

		return cachedValue.([]byte), nil
	}

	if item == nil {
		_ = k.StoreHashNilMember(identifier, key)
		return
	}

	cachedValue, err = tapao.Marshal(item, tapao.With(k.serializer))
	if err != nil {
		return
	}

	cacheItem := NewItem(key, cachedValue)
	for _, o := range opts {
		o(cacheItem)
	}
	_ = k.StoreHashMember(identifier, cacheItem)
	_ = k.StoreHashMemberFailover(identifier, cacheItem)

	return cachedValue.([]byte), nil
}

// GetHashMemberFailover :nodoc:
func (k *KeeperWithFailover) GetHashMemberFailover(identifier, key string) (cachedItem any, err error) {
	if k.disableCaching {
		return
	}

	cachedItem, err = getHashMember(k.failoverConnPool.Get(), identifier, key)
	if err != nil && err != ErrKeyNotExist && err != redigo.ErrNil || cachedItem != nil {
		return
	}

	return nil, nil
}

// StoreHashMemberFailover :nodoc:
func (k *KeeperWithFailover) StoreHashMemberFailover(identifier string, c Item) (err error) {
	if k.disableCaching {
		return nil
	}

	client := k.failoverConnPool.Get()
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
	_, err = client.Do("EXPIRE", identifier, k.failoverTTL.Seconds())
	if err != nil {
		return err
	}

	_, err = client.Do("EXEC")
	return
}
