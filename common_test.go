package cacher

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/kumparan/go-utils"

	"github.com/stretchr/testify/assert"
)

func Test_ParseCacheResultToPointerObject(t *testing.T) {
	type TestObj struct {
		ID int64
	}

	t.Run("ok, primitive types, string", func(t *testing.T) {
		data := "123"
		res, err := ParseCacheResultToPointerObject[string](utils.ToByte(data))
		assert.NoError(t, err)
		assert.Equal(t, *res, data)
	})

	t.Run("ok, primitive types, integer", func(t *testing.T) {
		data := int64(123)
		res, err := ParseCacheResultToPointerObject[int64](utils.ToByte(data))
		assert.NoError(t, err)
		assert.Equal(t, *res, data)
	})

	t.Run("ok, object", func(t *testing.T) {
		data := TestObj{ID: 1234}
		res, err := ParseCacheResultToPointerObject[TestObj](utils.ToByte(data))
		assert.NoError(t, err)
		assert.Equal(t, *res, data)
	})

	t.Run("ok, null cache", func(t *testing.T) {
		res, err := ParseCacheResultToPointerObject[TestObj]([]byte(`null`))
		assert.NoError(t, err)
		assert.Nil(t, res)
		// check null type are equal;
		assert.Equal(t, fmt.Sprintf("%T", &TestObj{}), fmt.Sprintf("%T", res))
	})

	t.Run("error, cache result are not byte ", func(t *testing.T) {
		res, err := ParseCacheResultToPointerObject[TestObj](int64(123456))
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, "failed to cast int64 to byte", err.Error())
	})

	t.Run("error, failed on unmarshal ", func(t *testing.T) {
		res, err := ParseCacheResultToPointerObject[TestObj](utils.ToByte([]int64{1, 2, 3, 4, 5, 6}))
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, "failed to unmarshal [1,2,3,4,5,6] to *cacher.TestObj", err.Error())
	})
}

func testCacheKey(key int64) string {
	return fmt.Sprintf("cacheKey:%d", key)
}

func TestStoreMapValues(t *testing.T) {
	k := NewKeeper()

	m, err := miniredis.Run()
	assert.NoError(t, err)

	r := newRedisConn(m.Addr())
	k.SetConnectionPool(r)
	k.SetLockConnectionPool(r)
	k.SetWaitTime(1 * time.Second)

	type TestObj struct {
		ID int64
	}

	t.Run("success", func(t *testing.T) {
		keys := []int64{1, 2, 3}
		buffer := make(map[int64]*TestObj)

		for _, key := range keys {
			buffer[key] = &TestObj{ID: key}
		}

		StoreCaches[int64, *TestObj](k, keys, buffer, testCacheKey)

		for _, key := range keys {
			assert.True(t, m.Exists(testCacheKey(key)))
			redisVal, err := k.Get(testCacheKey(key))
			assert.NoError(t, err)
			res, err := ParseCacheResultToPointerObject[TestObj](redisVal)
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, TestObj{ID: key}, *res)
		}
	})

	t.Run("success, some keys not exist in buffer", func(t *testing.T) {
		keys := []int64{4, 5, 6}
		buffer := make(map[int64]*TestObj)

		for _, key := range keys {
			buffer[key] = &TestObj{ID: key}
		}

		testKeys := []int64{4, 7, 8}

		StoreCaches[int64, *TestObj](k, []int64{4, 7, 8}, buffer, testCacheKey)

		for _, key := range testKeys {
			if key == 4 {
				assert.True(t, m.Exists(testCacheKey(key)))
				redisVal, err := k.Get(testCacheKey(key))
				assert.NoError(t, err)
				res, err := ParseCacheResultToPointerObject[TestObj](redisVal)
				assert.NoError(t, err)
				assert.NotNil(t, res)
				assert.Equal(t, TestObj{ID: 4}, *res)
				continue
			}

			assert.True(t, m.Exists(testCacheKey(key)))
			redisVal, err := k.Get(testCacheKey(key))
			assert.NoError(t, err)
			res, err := ParseCacheResultToPointerObject[TestObj](redisVal)
			assert.NoError(t, err)
			assert.Nil(t, res)
		}
	})
}
