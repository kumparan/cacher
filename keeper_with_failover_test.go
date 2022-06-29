package cacher

import (
	"errors"
	"testing"
	"time"

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

	val := "hey this is the result"

	t.Run("No cache", func(t *testing.T) {
		testKey := "just-a-key"
		assert.False(t, m.Exists(testKey))

		ttl := 1600 * time.Second
		retVal, err := k.GetOrSet(testKey, func() (i interface{}, e error) {
			return val, nil
		}, ttl)
		require.NoError(t, err)
		assert.EqualValues(t, val, retVal)
		assert.True(t, m.Exists(testKey))
		assert.True(t, mFO.Exists(testKey))
	})

	t.Run("Already cached", func(t *testing.T) {
		testKey := "just-a-key"
		assert.True(t, m.Exists(testKey))
		ttl := 1600 * time.Second
		retVal, err := k.GetOrSet(testKey, func() (i interface{}, e error) {
			return "thisis-not-expected", nil
		}, ttl)
		require.NoError(t, err)
		assert.EqualValues(t, val, retVal)
		assert.True(t, m.Exists(testKey))
	})

	t.Run("use failover", func(t *testing.T) {
		testKey := "just-a-key-failover"
		assert.False(t, m.Exists(testKey))
		err = mFO.Set(testKey, val)
		require.NoError(t, err)

		ttl := 1600 * time.Second
		retVal, err := k.GetOrSet(testKey, func() (i interface{}, e error) {
			return nil, errors.New("error")
		}, ttl)
		require.NoError(t, err)
		assert.EqualValues(t, val, retVal)
	})
}
