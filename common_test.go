package cacher

import (
	"fmt"
	"testing"

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
