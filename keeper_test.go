package cacher

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	"github.com/alicebob/miniredis"
)

func newRedisConn(url string) *redigo.Pool {
	return &redigo.Pool{
		MaxIdle:     100,
		MaxActive:   10000,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", url)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func TestCheckKeyExist(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	testKey := "test-key"

	t.Run("Not Exist", func(t *testing.T) {
		result, err := k.CheckKeyExist(testKey)
		assert.NoError(t, err)
		assert.EqualValues(t, result, false)
	})

	t.Run("Exist", func(t *testing.T) {
		val := "something-something-here"
		m.Set(testKey, val)
		result, err := k.CheckKeyExist(testKey)
		assert.NoError(t, err)
		assert.EqualValues(t, result, true)
	})
}

func TestGet(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	testKey := "test-key"

	t.Run("Not Exist", func(t *testing.T) {
		assert.False(t, m.Exists(testKey))
		result, err := k.Get(testKey)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("Exist", func(t *testing.T) {
		val := "something-something-here"
		m.Set(testKey, val)
		result, err := k.Get(testKey)
		assert.NoError(t, err)
		assert.EqualValues(t, result, val)
	})
}

func TestGetLockStore(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	testKey := "test-key"

	// It should return mutex when no other process is locking the process
	res, mu, err := k.GetOrLock(testKey)
	assert.Nil(t, res)
	assert.NoError(t, err)
	assert.NotNil(t, mu)

	// It should wait, and return an error while waiting for cached item ready
	res2, mu2, err2 := k.GetOrLock(testKey)
	assert.Nil(t, res2)
	assert.Nil(t, mu2)
	assert.Error(t, err2)

	// It should get response when mutex lock unlocked and cache item ready
	item := NewItem(testKey, "test-response")
	err = k.Store(mu, item)
	assert.NoError(t, err)

	res2, mu2, err2 = k.GetOrLock(testKey)
	assert.EqualValues(t, "test-response", res2)
	assert.Nil(t, mu2)
	assert.NoError(t, err2)
}

func TestGetOrSet(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	val := "hey this is the result"

	t.Run("No cache", func(t *testing.T) {
		testKey := "just-a-key"
		assert.False(t, m.Exists(testKey))

		ttl := 1600 * time.Second
		retVal, err := k.GetOrSet(testKey, func() (i interface{}, e error) {
			return val, nil
		}, time.Duration(ttl))
		assert.NoError(t, err)
		assert.EqualValues(t, val, retVal)
		assert.True(t, m.Exists(testKey))
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key"
		assert.True(t, m.Exists(testKey))
		ttl := 1600 * time.Second
		retVal, err := k.GetOrSet(testKey, func() (i interface{}, e error) {
			return "thisis-not-expected", nil
		}, time.Duration(ttl))
		assert.NoError(t, err)
		assert.EqualValues(t, val, retVal)
		assert.True(t, m.Exists(testKey))
	})
}

func TestPurge(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	// It should purge keys match with the matchstring while leaving the rest untouched
	testKeys := map[string]interface{}{
		"story:1234:comment:4321": nil,
		"story:1234:comment:4231": nil,
		"story:1234:comment:4121": nil,
		"story:1234:comment:4421": nil,
		"story:1234:comment:4521": nil,
		"story:1234:comment:4021": nil,
		"story:2000:comment:3021": "anything",
		"story:2000:comment:3421": "anything",
		"story:2000:comment:3231": "anything",
	}

	for key := range testKeys {
		_, mu, err := k.GetOrLock(key)
		assert.NoError(t, err)

		err = k.Store(mu, NewItem(key, "anything"))
		assert.NoError(t, err)
	}

	err = k.Purge("story:1234:*")
	assert.NoError(t, err)

	for key, value := range testKeys {
		res, _, err := k.GetOrLock(key)
		assert.NoError(t, err)
		assert.EqualValues(t, value, res)
	}
}

func TestDecideCacheTTL(t *testing.T) {
	k := &keeper{
		defaultTTL:   defaultTTL,
		lockDuration: defaultLockDuration,
		lockTries:    defaultLockTries,
		waitTime:     defaultWaitTime,
	}

	testKey := "test-key"

	// It should use keeper's default TTL when new cache item didn't specify the TTL
	i := NewItem(testKey, nil)
	assert.Equal(t, int64(k.defaultTTL.Seconds()), k.decideCacheTTL(i))

	// It should use specified TTL when new cache item specify the TTL
	i2 := NewItemWithCustomTTL(testKey, nil, 10*time.Second)
	assert.Equal(t, i2.GetTTLInt64(), k.decideCacheTTL(i))
}

func TestIncreaseCachedValueByOne(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	testKey := "increase-test"
	_, mu, err := k.GetOrLock(testKey)
	assert.NoError(t, err)

	err = k.Store(mu, NewItem(testKey, 0))
	assert.NoError(t, err)

	err = k.IncreaseCachedValueByOne(testKey)
	assert.NoError(t, err)

	reply, mu, err := k.GetOrLock(testKey)
	assert.NoError(t, err)

	var count int64
	bt, _ := reply.([]byte)
	err = json.Unmarshal(bt, &count)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, count)
}

func TestDeleteByKeys(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	// It should purge keys match with the matchstring while leaving the rest untouched
	testKeys := map[string]interface{}{
		"story:1234:comment:4321": "anything",
		"story:1234:comment:4231": "anything",
		"story:1234:comment:4121": "anything",
		"story:1234:comment:4421": "anything",
		"story:1234:comment:4521": "anything",
		"story:1234:comment:4021": "anything",
		"story:2000:comment:3021": "anything",
		"story:2000:comment:3421": "anything",
		"story:2000:comment:3231": "anything",
	}

	for key := range testKeys {
		_, mu, err := k.GetOrLock(key)
		assert.NoError(t, err)

		err = k.Store(mu, NewItem(key, "anything"))
		assert.NoError(t, err)
	}
	keys := []string{
		"story:1234:comment:4321",
		"story:1234:comment:4231",
		"story:1234:comment:4121",
		"story:1234:comment:4421",
		"story:1234:comment:4521",
		"story:1234:comment:4021",
		"story:2000:comment:3021",
		"story:2000:comment:3421",
		"story:2000:comment:3231",
	}
	err = k.DeleteByKeys(keys)
	assert.NoError(t, err)

	for _, key := range keys {
		res, _, err := k.GetOrLock(key)
		assert.NoError(t, err)
		assert.EqualValues(t, nil, res)
	}
}

func TestStoreMultiWithoutBlocking(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	// It should purge keys match with the matchstring while leaving the rest untouched
	testKeys := map[string]interface{}{
		"story:1234:comment:4321": "anything1",
		"story:1234:comment:4231": "anything2",
		"story:1234:comment:4121": "anything3",
		"story:1234:comment:4421": "anything4",
		"story:1234:comment:4521": "anything5",
		"story:1234:comment:4021": "anything6",
		"story:2000:comment:3021": "anything7",
		"story:2000:comment:3421": "anything8",
		"story:2000:comment:3231": "anything9",
	}

	items := []Item{}
	for key, value := range testKeys {
		items = append(items, NewItem(key, value))
	}

	err = k.StoreMultiWithoutBlocking(items)
	assert.NoError(t, err)

	for key, value := range testKeys {
		res, _, err := k.GetOrLock(key)
		assert.NoError(t, err)
		assert.EqualValues(t, value, res)
	}
}

func TestStoreMultiPersist(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	err = k.StoreMultiPersist([]Item{
		NewItem("abc", "hehehe"),
		NewItem("def", "hohoho"),
	})

	assert.True(t, m.Exists("abc"))
	assert.True(t, m.Exists("def"))

	assert.EqualValues(t, 0, m.TTL("abc"))
	assert.EqualValues(t, 0, m.TTL("asdfasd"))
}

func TestExpire(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	key := "combro"

	m.Set(key, "enaq scully")
	m.SetTTL(key, 60*time.Second)

	newTTL := 66 * time.Hour
	k.Expire(key, newTTL)

	assert.EqualValues(t, newTTL, m.TTL(key))
}

func TestExpireMulti(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	key1 := "combro"
	key2 := "kuelapis"

	m.Set(key1, "enaq scully")
	m.SetTTL(key1, 5*time.Second)
	m.Set(key2, "mantul over 9000")
	m.SetTTL(key2, 9*time.Second)

	newTTL1 := 66 * time.Hour
	newTTL2 := 77 * time.Hour

	k.ExpireMulti(map[string]time.Duration{
		key1: newTTL1,
		key2: newTTL2,
	})

	assert.EqualValues(t, newTTL1, m.TTL(key1))
	assert.EqualValues(t, newTTL2, m.TTL(key2))
}

func TestGetLockStoreRightLeftList(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	name := "list-name"

	var multiList []string

	multiList = append(multiList, "test-response")
	err = k.StoreLeftList(name, "test-response")
	assert.NoError(t, err)

	multiList = append(multiList, "test-response-2")
	err = k.StoreRightList(name, "test-response-2")
	assert.NoError(t, err)

	res2, err2 := k.GetList(name, 2, 1)
	resultList, err := redis.Strings(res2, nil)
	assert.EqualValues(t, multiList, resultList)
	assert.NoError(t, err2)

}

func TestGetAndRemoveFirstAndLastListElement(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	name := "list-name"

	err = k.StoreRightList(name, "test-response")
	assert.NoError(t, err)

	err = k.StoreRightList(name, "test-response-2")
	assert.NoError(t, err)

	err = k.StoreRightList(name, "test-response-3")
	assert.NoError(t, err)

	res3, err3 := k.GetAndRemoveFirstListElement(name)
	firstElement, err := redis.String(res3, nil)
	assert.EqualValues(t, firstElement, "test-response")
	assert.NoError(t, err3)

	res4, err4 := k.GetAndRemoveLastListElement(name)
	lastElement, err := redis.String(res4, nil)
	assert.EqualValues(t, lastElement, "test-response-3")
	assert.NoError(t, err4)

}
func TestGetListLength(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	name := "list-name"

	err = k.StoreRightList(name, "test-response")
	assert.NoError(t, err)

	err = k.StoreRightList(name, "test-response-2")
	assert.NoError(t, err)

	res3, err3 := k.GetListLength(name)
	assert.EqualValues(t, res3, 2)
	assert.NoError(t, err3)

}
func TestGetTTL(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second) // override wait time to 1 second

	testKey := "list-name"

	itemWithTTL := NewItemWithCustomTTL(testKey, nil, 100*time.Second)
	err = k.StoreWithoutBlocking(itemWithTTL)
	assert.NoError(t, err)

	ttl, err := k.GetTTL(testKey)
	assert.NoError(t, err)
	assert.NotEqual(t, ttl, 0)
	var typeInt64 int64
	assert.IsType(t, typeInt64, ttl)

}

func TestStoreNil(t *testing.T) {
	k := NewKeeper()
	m, err := miniredis.Run()

	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second)

	testKey := "test-key"

	err = k.StoreNil(testKey, 5*time.Minute)
	assert.NoError(t, err)

	reply, mu, err := k.GetOrLock(testKey)

	assert.NoError(t, err)
	assert.Equal(t, []byte("null"), reply)
	assert.Nil(t, mu)
}
