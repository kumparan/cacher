package cacher

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/kumparan/tapao"
	"github.com/stretchr/testify/require"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	"github.com/alicebob/miniredis/v2"
)

type TestStruct struct {
	TestString     string
	TestInt64      int64
	TestFloat64    float64
	TestTime       time.Time
	TestNilString  *string
	TestNilInt64   *int64
	TestNilFloat64 *float64
	TestNilTime    *time.Time
}

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
		_ = m.Set(testKey, val)
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
		_ = m.Set(testKey, val)
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

	val := TestStruct{
		TestString:     "string",
		TestInt64:      1640995120740899877,
		TestFloat64:    234.23324,
		TestTime:       time.UnixMilli(3276483223),
		TestNilString:  nil,
		TestNilInt64:   nil,
		TestNilFloat64: nil,
		TestNilTime:    nil,
	}

	valByte, err := tapao.Marshal(val, tapao.With(tapao.JSON))
	require.NoError(t, err)

	t.Run("No cache", func(t *testing.T) {
		testKey := "just-a-key"
		assert.False(t, m.Exists(testKey))

		retVal, err := k.GetOrSet(testKey, func() (any, error) {
			return val, nil
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.EqualValues(t, val, myVar)
		assert.True(t, m.Exists(testKey))

		cachedValue, err := m.Get(testKey)
		require.NoError(t, err)
		assert.Equal(t, string(valByte), cachedValue)
		assert.True(t, m.Exists(testKey))
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key"
		err := m.Set(testKey, string(valByte))
		require.NoError(t, err)

		retVal, err := k.GetOrSet(testKey, func() (any, error) {
			return "thisis-not-expected", nil
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.EqualValues(t, val, myVar)
	})

	t.Run("Already cached, nil value", func(t *testing.T) {
		testKey := "just-a-key-nil"
		err := m.Set(testKey, "null")
		require.NoError(t, err)

		retVal, err := k.GetOrSet(testKey, func() (any, error) {
			return "thisis-not-expected", nil
		})
		require.NoError(t, err)

		var myVar *TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.Nil(t, myVar)
	})
}

func TestGetHashMemberOrSet(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	val := TestStruct{
		TestString:     "string",
		TestInt64:      1640995120740899877,
		TestFloat64:    234.23324,
		TestTime:       time.UnixMilli(3276483223),
		TestNilString:  nil,
		TestNilInt64:   nil,
		TestNilFloat64: nil,
		TestNilTime:    nil,
	}

	valByte, err := tapao.Marshal(val, tapao.With(tapao.JSON))
	require.NoError(t, err)

	identifier := "this-is-identifier"

	t.Run("No cache", func(t *testing.T) {
		testKey := "just-a-key"
		retVal, err := k.GetHashMemberOrSet(identifier, testKey, func() (any, error) {
			return val, nil
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.EqualValues(t, val, myVar)

		cachedValue := m.HGet(identifier, testKey)
		assert.Equal(t, string(valByte), cachedValue)
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key"
		m.HSet(identifier, testKey, string(valByte))

		retVal, err := k.GetHashMemberOrSet(identifier, testKey, func() (any, error) {
			return "thisis-not-expected", nil
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.EqualValues(t, val, myVar)
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
	testKeys := map[string]any{
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

	reply, _, err := k.GetOrLock(testKey)
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
	testKeys := map[string]any{
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
	testKeys := map[string]any{
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

	_ = k.StoreMultiPersist([]Item{
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

	_ = m.Set(key, "enaq scully")
	m.SetTTL(key, 60*time.Second)

	newTTL := 66 * time.Hour
	_ = k.Expire(key, newTTL)

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

	_ = m.Set(key1, "enaq scully")
	m.SetTTL(key1, 5*time.Second)
	_ = m.Set(key2, "mantul over 9000")
	m.SetTTL(key2, 9*time.Second)

	newTTL1 := 66 * time.Hour
	newTTL2 := 77 * time.Hour

	_ = k.ExpireMulti(map[string]time.Duration{
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
	resultList, _ := redigo.Strings(res2, nil)
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
	firstElement, _ := redigo.String(res3, nil)
	assert.EqualValues(t, firstElement, "test-response")
	assert.NoError(t, err3)

	res4, err4 := k.GetAndRemoveLastListElement(name)
	lastElement, _ := redigo.String(res4, nil)
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

	err = k.StoreNil(testKey)
	assert.NoError(t, err)

	reply, mu, err := k.GetOrLock(testKey)

	assert.NoError(t, err)
	assert.Equal(t, []byte("null"), reply)
	assert.Nil(t, mu)
}

func TestStoreHashNilMember(t *testing.T) {
	k := NewKeeper()
	m, err := miniredis.Run()

	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second)

	identifier := "identifier"
	testKey := "test-key"

	err = k.StoreHashNilMember(identifier, testKey)
	assert.NoError(t, err)

	reply, mu, err := k.GetHashMemberOrLock(identifier, testKey)

	assert.NoError(t, err)
	assert.Equal(t, []byte("null"), reply)
	assert.Nil(t, mu)
}

func TestGetOrLock(t *testing.T) {
	t.Run("cache miss", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(1 * time.Second)

		key := "walawaladumdum"

		result, mu, err := k.GetOrLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// Nothing from redis
		assert.Nil(t, result)
		// We got a lock
		assert.NotNil(t, mu)
	})

	t.Run("locked got nil", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(2 * time.Second)

		key := "walawaladumdum"

		// lock it
		mu1, err := k.AcquireLock(key)
		if err != nil {
			t.Fatal(err)
		}

		// release lock in 0.5 sec
		go func() {
			time.Sleep(500 * time.Millisecond)
			// Make sure it's empty
			err2 := k.DeleteByKeys([]string{key})
			assert.NoError(t, err2)
			_, _ = mu1.Unlock()
		}()

		t1 := time.Now()
		result, mu2, err := k.GetOrLock(key)
		d := time.Since(t1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, result)
		// We got a new lock
		assert.NotNil(t, mu2)
		assert.True(t, d >= (500*time.Millisecond))
		assert.True(t, d < (1*time.Second))
		assert.NotEqual(t, mu1, mu2)
	})

	t.Run("locked got result", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(2 * time.Second)

		key := "walawaladumdum"
		testVal := "awokwokwok"

		// lock it
		mu1, err := k.AcquireLock(key)
		if err != nil {
			t.Fatal(err)
		}

		// release lock in 0.5 sec
		go func() {
			time.Sleep(500 * time.Millisecond)
			// Now fill it with cache
			err2 := k.StoreWithoutBlocking(NewItem(key, testVal))
			assert.NoError(t, err2)
			_, _ = mu1.Unlock()
		}()

		t1 := time.Now()
		result, mu2, err := k.GetOrLock(key)
		d := time.Since(t1)
		if err != nil {
			t.Fatal(err)
		}
		assert.NotNil(t, result)
		assert.Equal(t, []byte(testVal), result)
		// Look ma, no lock
		assert.Nil(t, mu2)
		assert.True(t, d >= (500*time.Millisecond))
		assert.True(t, d < (1*time.Second))
		assert.NotEqual(t, mu1, mu2)
	})

	t.Run("locked got wait too long", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()

		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(500 * time.Millisecond)

		key := "walawaladumdum"

		// lock it
		_, err = k.AcquireLock(key)
		if err != nil {
			t.Fatal(err)
		}

		result, mu2, err := k.GetOrLock(key)
		assert.Equal(t, ErrWaitTooLong, err)
		assert.Nil(t, result)
		assert.Nil(t, mu2)
	})
}

func TestGetHashMemberOrLock(t *testing.T) {
	t.Run("cache miss", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()

		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(1 * time.Second)

		id := "this-is-some-bucket"
		key := "walawaladumdum"

		result, mu, err := k.GetHashMemberOrLock(id, key)
		if err != nil {
			t.Fatal(err)
		}
		// Nothing from redis
		assert.Nil(t, result)
		// We got a lock
		assert.NotNil(t, mu)
	})

	t.Run("locked got nil", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()

		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(2 * time.Second)

		id := "this-is-some-bucket"
		key := "walawaladumdum"

		// lock it
		mu1, err := k.AcquireLock(fmt.Sprintf("%s:%s", id, key))
		if err != nil {
			t.Fatal(err)
		}

		// release lock in 0.5 sec
		go func() {
			time.Sleep(500 * time.Millisecond)
			// Make sure it's empty
			err2 := k.DeleteByKeys([]string{id})
			assert.NoError(t, err2)
			_, _ = mu1.Unlock()
		}()

		t1 := time.Now()
		result, mu2, err := k.GetHashMemberOrLock(id, key)
		d := time.Since(t1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, result)
		assert.NotNil(t, mu2)
		assert.True(t, d >= (500*time.Millisecond))
		assert.True(t, d < (1*time.Second))
		assert.NotEqual(t, mu1, mu2)
	})

	t.Run("locked got result", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()

		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(2 * time.Second)

		id := "this-is-some-bucket"
		key := "walawaladumdum"
		testVal := "awokwokwok"

		// lock it
		mu1, err := k.AcquireLock(fmt.Sprintf("%s:%s", id, key))
		if err != nil {
			t.Fatal(err)
		}

		// release lock in 0.5 sec
		go func() {
			time.Sleep(500 * time.Millisecond)
			// Now fill it with cache
			err2 := k.StoreHashMember(id, NewItem(key, testVal))
			assert.NoError(t, err2)
			_, _ = mu1.Unlock()
		}()

		t1 := time.Now()
		result, mu2, err := k.GetHashMemberOrLock(id, key)
		d := time.Since(t1)
		if err != nil {
			t.Fatal(err)
		}
		assert.NotNil(t, result)
		assert.Equal(t, []byte(testVal), result)
		assert.Nil(t, mu2)
		assert.True(t, d >= (500*time.Millisecond))
		assert.True(t, d < (1*time.Second))
		assert.NotEqual(t, mu1, mu2)
	})

	t.Run("locked got wait too long", func(t *testing.T) {
		k := NewKeeper()
		m, err := miniredis.Run()

		if err != nil {
			t.Fatal(err)
		}

		r := newRedisConn(m.Addr())
		k.SetConnectionPool(r)
		k.SetLockConnectionPool(r)
		k.SetWaitTime(500 * time.Millisecond)

		id := "this-is-some-bucket"
		key := "walawaladumdum"

		// lock it
		_, err = k.AcquireLock(fmt.Sprintf("%s:%s", id, key))
		if err != nil {
			t.Fatal(err)
		}

		result, mu2, err := k.GetHashMemberOrLock(id, key)
		assert.Equal(t, ErrWaitTooLong, err)
		assert.Nil(t, result)
		assert.Nil(t, mu2)
	})
}

func TestIncreaseHashMemberValue(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	testKey := "increase-test"
	bucketKey := "bucket-test"
	_, mu, err := k.GetHashMemberOrLock(bucketKey, testKey)
	assert.NoError(t, err)

	err = k.Store(mu, NewItem(testKey, 0))
	assert.NoError(t, err)

	count, err := k.IncreaseHashMemberValue(bucketKey, testKey, 5)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, count)
}

func TestGetHashMemberThenDelete(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	bucketKey := "bucket-test"

	// when hset 2 keys
	err = k.StoreHashMember(bucketKey, NewItem("key1", int64(1000)))
	assert.NoError(t, err)

	err = k.StoreHashMember(bucketKey, NewItem("key2", int64(2000)))
	assert.NoError(t, err)

	keys, err := m.HKeys(bucketKey)
	assert.NoError(t, err)
	assert.EqualValues(t, len(keys), 2)

	// then call GetHashMemberThenDelete
	rep, err := k.GetHashMemberThenDelete(bucketKey, "key1")
	assert.NoError(t, err)

	// should reply value without error
	strRep := string(rep.([]byte))
	valInt, err := strconv.ParseInt(strRep, 10, 64)
	assert.NoError(t, err)
	assert.EqualValues(t, 1000, valInt)

	// and the keys decreased to 1
	keys, err = m.HKeys(bucketKey)
	assert.NoError(t, err)
	assert.EqualValues(t, len(keys), 1)
}

func TestGetHashMemberThenDelete_Empty(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)
	defer m.Close()

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	// when the bucket is empty
	bucketKey := "bucket-test"
	assert.False(t, m.Exists(bucketKey))

	// and call GetHashMemberThenDelete
	rep, err := k.GetHashMemberThenDelete(bucketKey, "key1")
	assert.NoError(t, err)
	assert.Nil(t, rep)
}

func TestHashScan(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	bucketKey := "bucket-test"

	// when hset 3 keys
	cases := map[string]int{
		"key1": 1000,
		"key2": 2000,
		"key3": 3000,
	}

	for key, val := range cases {
		err = k.StoreHashMember(bucketKey, NewItem(key, val))
		assert.NoError(t, err)
	}

	// then call HashScan
	_, result, err := k.HashScan(bucketKey, 0)
	assert.NoError(t, err)
	for k, v := range result {
		assert.EqualValues(t, fmt.Sprint(cases[k]), v)
	}
}

func TestHashScan_Empty(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)

	// when bucket is empty
	bucketKey := "bucket-test"
	// and call HashScan
	cursor, result, err := k.HashScan(bucketKey, 0)
	assert.NoError(t, err)
	assert.Empty(t, result)
	assert.EqualValues(t, 0, cursor)
}
