package cacher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client *redis.Client

func newTestKeeper() *keeper {
	return &keeper{
		connPool:       client,
		lockConnPool:   client,
		disableCaching: false,
		lockDuration:   defaultLockDuration,
		defaultTTL:     defaultTTL,
		nilTTL:         defaultNilTTL,
		waitTime:       defaultWaitTime,
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	s := strings.Repeat("hello", 300)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a := StringToBytes(s)
		_ = a
	}
}

func TestMain(m *testing.M) {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	client = redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	i := m.Run()
	mr.Close()

	os.Exit(i)
}

func TestKeeper_AcquireLock(t *testing.T) {
	keeper := newTestKeeper()
	key := "test"
	ctx := context.TODO()
	lock, err := keeper.AcquireLock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if lock == nil {
		t.Fatal("lock shouldn't be nil")
	}

	lock2, err := keeper.AcquireLock(ctx, key)
	if err == nil {
		t.Fatal("should be error lock not obtained")
	}

	if lock2 != nil {
		t.Fatal("lock 2 should be nil")
	}

	err = lock.Release(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKeeper_Get(t *testing.T) {
	keeper := newTestKeeper()
	key := "test"
	ctx := context.TODO()

	res, err := keeper.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if res != nil {
		t.Fatal("result should be nil")
	}

	cmd := client.Set(ctx, key, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	res, err = keeper.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal("result should not be nil")
	}
}

func TestKeeper_GetOrLock(t *testing.T) {
	ctx := context.TODO()
	t.Run("getting the lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock"

		res, lock, err := keeper.GetOrLock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if lock == nil {
			t.Fatal("should getting the lock")
		}
		if res != nil {
			t.Fatal("result should be nil")
		}
		if err := lock.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wait to getting lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock"

		cmd := client.Set(ctx, "lock:"+key, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetOrLock(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if lock != nil {
				t.Fatal("should not getting the lock")
			}
			if res == nil {
				t.Fatal("result should not be nil")
			}
			b, ok := res.([]byte)
			if !ok {
				t.Fatal("failed to casting to bytes")
			}
			if string(b) != "test" {
				t.Fatal("invalid value")
			}
		}()

		cmd = client.Set(ctx, key, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		dumpCmd := client.Del(ctx, "lock:"+key)
		if dumpCmd.Err() != nil {
			t.Fatal(dumpCmd.Err())
		}

		<-doneCh
	})

	t.Run("locked got nil, then acquire lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock-but-nil"
		lockKey := "lock:" + key
		cmd := client.Set(ctx, lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetOrLock(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if lock == nil {
				t.Fatal("should getting the new lock")
			}
			if res != nil {
				t.Fatal("result should be nil")
			}

			if err := lock.Release(ctx); err != nil {
				t.Fatal("should not error")
			}
		}()

		// delete the lock key, but not set a value to the key
		delCmd := client.Del(ctx, lockKey)
		if delCmd.Err() != nil {
			t.Fatal("should not error")
		}

		<-doneCh
	})

	t.Run("error wait too long", func(t *testing.T) {
		keeper := newTestKeeper()
		keeper.SetWaitTime(100 * time.Millisecond)
		key := "test-get-or-lock-but-error-wait-too-ling"
		lockKey := "lock:" + key

		cmd := client.Set(ctx, lockKey, []byte("test"), 200*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		res, lock, err := keeper.GetOrLock(ctx, key)
		if err == nil {
			t.Fatal("should error")
		}

		if err != ErrWaitTooLong {
			t.Fatal("should error wait too long")
		}

		if lock != nil {
			t.Fatal("should be nil")
		}

		if res != nil {
			t.Fatal("should be nil")
		}
	})
}

func TestKeeper_GetOrSet(t *testing.T) {
	keeper := newTestKeeper()
	key := "test-get-or-set"
	ctx := context.TODO()
	res, err := keeper.GetOrSet(ctx, key, func() (interface{}, error) {
		return []byte("test"), nil
	}, defaultTTL)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("result should not be nil")
	}

	// second call, getting data from cache
	res, err = keeper.GetOrSet(ctx, key, nil, defaultTTL)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("result should not be nil")
	}
}

func TestKeeper_CheckKeyExist(t *testing.T) {
	keeper := newTestKeeper()
	key := "test-key-exists"
	ctx := context.TODO()

	exists, err := keeper.CheckKeyExist(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("should not exists")
	}

	cmd := client.Set(ctx, key, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	exists, err = keeper.CheckKeyExist(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("should exists")
	}
}

func TestKeeper_DeleteByKeys(t *testing.T) {
	keeper := newTestKeeper()
	key1 := "test-delete-keys-1"
	ctx := context.TODO()

	cmd := client.Set(ctx, key1, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	key2 := "test-delete-keys-2"
	cmd = client.Set(ctx, key2, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	err := keeper.DeleteByKeys(ctx, []string{key1, key2})
	if err != nil {
		t.Fatal(err)
	}

	exCmd := client.Exists(ctx, key1, key2)
	if exCmd.Err() != nil {
		t.Fatal(exCmd.Err())
	}
	if exCmd.Val() != 0 {
		t.Fatal("should 0")
	}
}

func TestKeeper_GetHashMember(t *testing.T) {
	keeper := newTestKeeper()

	bucket := "test-bucket"
	key := "test-get-hash-member"
	ctx := context.TODO()

	res, err := keeper.GetHashMember(ctx, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("should be nil")
	}

	bCmd := client.HSet(ctx, bucket, key, []byte("test"))
	if bCmd.Err() != nil {
		t.Fatal(bCmd)
	}

	res, err = keeper.GetHashMember(ctx, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("should not be nil")
	}
}

func TestKeeper_GetHashMemberOrLock(t *testing.T) {
	ctx := context.TODO()
	t.Run("getting the lock", func(t *testing.T) {
		keeper := newTestKeeper()
		bucket := "test-bucket-hash"
		key := "test-get-or-lock-hash"

		res, lock, err := keeper.GetHashMemberOrLock(ctx, bucket, key)
		if err != nil {
			t.Fatal(err)
		}
		if lock == nil {
			t.Fatal("should getting the lock")
		}
		if res != nil {
			t.Fatal("result should be nil")
		}

		err = lock.Release(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wait to getting lock", func(t *testing.T) {
		keeper := newTestKeeper()
		bucket := "test-bucket"
		key := "test-get-or-lockKey"
		lockKey := fmt.Sprintf("lockKey:%s:%s", bucket, key)

		cmd := client.Set(ctx, lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetHashMemberOrLock(ctx, bucket, key)
			if err != nil {
				t.Fatal(err)
			}
			if lock != nil {
				t.Fatal("should not getting the lock")
			}
			if res == nil {
				t.Fatal("result should not be nil")
			}
			b, ok := res.([]byte)
			if !ok {
				t.Fatal("failed to casting to bytes")
			}
			if string(b) != "test" {
				t.Fatal("invalid value")
			}
		}()

		hsetCmd := client.HSet(ctx, bucket, key, []byte("test"))
		if hsetCmd.Err() != nil {
			t.Fatal(hsetCmd.Err())
		}

		dumpCmd := client.Del(ctx, lockKey)
		if dumpCmd.Err() != nil {
			t.Fatal(dumpCmd.Err())
		}

		<-doneCh
	})

	t.Run("locked got nil, then acquire lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock-but-nil"
		bucket := "test-hash-lock-got-nil-bucket"
		lockKey := fmt.Sprintf("lock:%s:%s", bucket, key)

		cmd := client.Set(ctx, lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetHashMemberOrLock(ctx, bucket, key)
			if err != nil {
				t.Fatal(err)
			}
			if lock == nil {
				t.Fatal("should getting the new lock")
			}
			if res != nil {
				t.Fatal("result should be nil")
			}

			if err := lock.Release(ctx); err != nil {
				t.Fatal("should not error")
			}
		}()

		// delete the lock key, but not set a value to the key
		delCmd := client.Del(ctx, lockKey)
		if delCmd.Err() != nil {
			t.Fatal("should not error")
		}

		<-doneCh
	})

	t.Run("error wait too long", func(t *testing.T) {
		keeper := newTestKeeper()
		keeper.SetWaitTime(100 * time.Millisecond)
		key := "test-get-or-lock-but-error-wait-too-ling"
		bucket := "test-hash-error-wait-too-long-bucket"
		lockKey := fmt.Sprintf("lock:%s:%s", bucket, key)

		cmd := client.Set(ctx, lockKey, []byte("test"), 200*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		res, lock, err := keeper.GetHashMemberOrLock(ctx, bucket, key)
		if err == nil {
			t.Fatal("should error")
		}

		if err != ErrWaitTooLong {
			t.Fatal("should error wait too long")
		}

		if lock != nil {
			t.Fatal("should be nil")
		}

		if res != nil {
			t.Fatal("should be nil")
		}
	})
}

func TestKeeper_DeleteHashMember(t *testing.T) {
	keeper := newTestKeeper()
	bucket := "test-bucket-hash-member"
	key1 := "test-get-or-lock"
	key2 := "test-get-or-lock-2"
	ctx := context.TODO()

	hsetCmd := client.HSet(ctx, bucket, key1, []byte("test"))
	if hsetCmd.Err() != nil {
		t.Fatal(hsetCmd.Err())
	}

	hsetCmd = client.HSet(ctx, bucket, key2, []byte("test2"))
	if hsetCmd.Err() != nil {
		t.Fatal(hsetCmd.Err())
	}

	err := keeper.DeleteHashMember(ctx, bucket, key1)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.DeleteHashMember(ctx, bucket, key2)
	if err != nil {
		t.Fatal(err)
	}

	bCmd := client.HGet(ctx, bucket, key2)
	if bCmd.Err() == nil || bCmd.Err() != redis.Nil {
		t.Fatal("should be error and nil error")
	}
}

func TestKeeper_Store(t *testing.T) {
	keeper := newTestKeeper()
	key := "test-store"
	ctx := context.TODO()

	bCmd := client.Exists(ctx, key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	lock, err := keeper.AcquireLock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.Store(ctx, lock, NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(ctx, key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 1 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_StoreWithoutBlocking(t *testing.T) {
	keeper := newTestKeeper()
	key := "test-store-without-blocking"
	ctx := context.TODO()

	bCmd := client.Exists(ctx, key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreWithoutBlocking(ctx, NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(ctx, key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 1 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_StoreHashMember(t *testing.T) {
	keeper := newTestKeeper()
	bucket := "test-bucket-store"
	key := "test-store-without-blocking"
	ctx := context.TODO()

	bCmd := client.Exists(ctx, bucket)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreHashMember(ctx, bucket, NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(ctx, bucket)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 1 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_StoreMultiHashMembers(t *testing.T) {
	item1 := NewItem("key1", "value1")
	item2 := NewItem("key2", "value2")
	m := map[string][]Item{
		"identifier1": {item1, item2},
		"identifier2": {item2},
	}
	keeper := newTestKeeper()
	ctx := context.TODO()

	err := keeper.StoreMultiHashMembers(ctx, m)
	assert.NoError(t, err)

	for id, items := range m {
		for _, item := range items {
			val, err := keeper.GetHashMember(ctx, id, item.GetKey())
			assert.NoError(t, err)
			assert.EqualValues(t, item.GetValue(), val)
		}
	}
}

func TestKeeper_StoreMultiWithoutBlocking(t *testing.T) {
	ctx := context.TODO()

	keeper := newTestKeeper()
	item1 := &item{
		key:   "key-1",
		ttl:   defaultTTL,
		value: []byte("test-1"),
	}
	item2 := &item{
		key:   "key-2",
		ttl:   defaultTTL,
		value: []byte("test-2"),
	}

	bCmd := client.Exists(ctx, item1.key, item2.key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreMultiWithoutBlocking(ctx, []Item{item1, item2})
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(ctx, item1.key, item2.key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 2 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_Persist(t *testing.T) {
	ctx := context.TODO()

	keeper := newTestKeeper()
	i := &item{
		key:   "tobe-persist",
		ttl:   defaultTTL,
		value: []byte("test-1"),
	}

	replyInt := client.Exists(ctx, i.key)
	if replyInt.Err() != nil {
		t.Fatal(replyInt.Err())
	}

	if replyInt.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreWithoutBlocking(ctx, i)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.Persist(ctx, i.key)
	if err != nil {
		t.Fatal(err)
	}

	replyDur := client.TTL(ctx, i.key)
	if replyDur.Err() != nil {
		t.Fatal(replyDur.Err())
	}

	if replyDur.Val() != -1 {
		t.Fatalf("expected: -1, got: %d", replyDur.Val())
	}
}

func TestKeeper_GetMultiHashMembers(t *testing.T) {
	ctx := context.TODO()

	t.Run("success", func(t *testing.T) {
		keeper := newTestKeeper()
		var hasMembers []HashMember
		bucket := "bucket"
		var members []string
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			members = append(members, key)
			hasMembers = append(hasMembers, HashMember{
				Identifier: bucket,
				Key:        key,
			})
			err := keeper.StoreHashMember(ctx, bucket, &item{
				key:   key,
				ttl:   defaultTTL,
				value: []byte(key),
			})
			assert.NoError(t, err)
		}

		replies, err := keeper.GetMultiHashMembers(ctx, hasMembers)
		assert.NoError(t, err)

		// make sure all keys is found
		assert.Equal(t, len(hasMembers), len(replies))

		for _, reply := range replies {
			strCMD, ok := reply.(*redis.StringCmd)
			assert.True(t, ok)
			val, err := strCMD.Result()
			assert.NoError(t, err)
			assert.Contains(t, members, val)
		}
	})

	t.Run("missing some keys", func(t *testing.T) {
		keeper := newTestKeeper()
		var hashMembers []HashMember
		bucket := "bucket"
		var keys []string
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			keys = append(keys, key)
			err := keeper.StoreHashMember(ctx, bucket, &item{
				key:   key,
				ttl:   defaultTTL,
				value: []byte(key),
			})
			assert.NoError(t, err)
		}

		keysWithMissings := make([]string, len(keys))
		copy(keysWithMissings, keys)
		missingKeys := []string{"11", "12", "13"}
		keysWithMissings = append(keysWithMissings, missingKeys...)
		for _, key := range keysWithMissings {
			hashMembers = append(hashMembers, HashMember{
				Identifier: bucket,
				Key:        key,
			})
		}

		replies, err := keeper.GetMultiHashMembers(ctx, hashMembers)
		assert.NoError(t, err)
		assert.Equal(t, len(hashMembers), len(replies))

		// make sure replies is sorted like
		for i := range keysWithMissings {
			strCMD, ok := replies[i].(*redis.StringCmd)
			assert.True(t, ok)
			val, err := strCMD.Result()
			if val == "" {
				assert.EqualError(t, redis.Nil, err.Error())
				assert.Equal(t, keysWithMissings[i], strCMD.Args()[2])
				continue
			}

			assert.NoError(t, err)
			assert.Equal(t, keys[i], val)
		}
	})
}
