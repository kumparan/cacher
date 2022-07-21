package cacher

import (
	"errors"
	"testing"
	"time"

	"github.com/kumparan/tapao"

	"github.com/stretchr/testify/require"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func Test_keeperWithFailover_GetOrSet(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeperWithFailover()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	mFO, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	rFO := newRedisConn(mFO.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetFailoverConnectionPool(rFO)
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
		testKey := "just-a-key-1"
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
		assert.True(t, mFO.Exists(testKey))
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key-2"
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

	t.Run("use failover", func(t *testing.T) {
		testKey := "just-a-key-failover-1"
		err = mFO.Set(testKey, string(valByte))
		require.NoError(t, err)

		retVal, err := k.GetOrSet(testKey, func() (i any, e error) {
			return nil, errors.New("error")
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)
		assert.EqualValues(t, val, myVar)
	})
}

func Test_keeperWithFailover_GetHashMemberOrSet(t *testing.T) {
	// Initialize new cache keeper
	k := NewKeeperWithFailover()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	mFO, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	rFO := newRedisConn(mFO.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetFailoverConnectionPool(rFO)
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
		testKey := "just-a-key-3"
		assert.False(t, m.Exists(identifier))

		retVal, err := k.GetHashMemberOrSet(identifier, testKey, func() (any, error) {
			return val, nil
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)

		assert.EqualValues(t, val, myVar)
		assert.True(t, m.Exists(identifier))
		assert.True(t, mFO.Exists(identifier))

		cachedValue := m.HGet(identifier, testKey)
		assert.Equal(t, string(valByte), cachedValue)

		cachedValue = mFO.HGet(identifier, testKey)
		assert.Equal(t, string(valByte), cachedValue)
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key-4"
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

	t.Run("use failover", func(t *testing.T) {
		testKey := "just-a-key-failover-2"
		mFO.HSet(identifier, testKey, string(valByte))

		retVal, err := k.GetHashMemberOrSet(identifier, testKey, func() (i any, e error) {
			return nil, errors.New("error")
		})
		require.NoError(t, err)

		var myVar TestStruct
		err = tapao.Unmarshal(retVal, &myVar, tapao.With(tapao.JSON))
		require.NoError(t, err)
		assert.EqualValues(t, val, myVar)
	})
}
