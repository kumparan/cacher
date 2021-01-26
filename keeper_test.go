package cacher

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
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
	lock, err := keeper.AcquireLock(key)
	if err != nil {
		t.Fatal(err)
	}

	if lock == nil {
		t.Fatal("lock shouldn't be nil")
	}

	lock2, err := keeper.AcquireLock(key)
	if err == nil {
		t.Fatal("should be error lock not obtained")
	}

	if lock2 != nil {
		t.Fatal("lock 2 should be nil")
	}

	err = lock.Release()
	if err != nil {
		t.Fatal(err)
	}
}

func TestKeeper_Get(t *testing.T) {
	keeper := newTestKeeper()
	key := "test"
	res, err := keeper.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if res != nil {
		t.Fatal("result should be nil")
	}

	cmd := client.Set(key, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	res, err = keeper.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal("result should not be nil")
	}
}

func TestKeeper_GetOrLock(t *testing.T) {
	t.Run("getting the lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock"

		res, lock, err := keeper.GetOrLock(key)
		if err != nil {
			t.Fatal(err)
		}
		if lock == nil {
			t.Fatal("should getting the lock")
		}
		if res != nil {
			t.Fatal("result should be nil")
		}
		if err := lock.Release(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wait to getting lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock"

		cmd := client.Set("lock:"+key, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetOrLock(key)
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

		cmd = client.Set(key, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		dumpCmd := client.Del("lock:" + key)
		if dumpCmd.Err() != nil {
			t.Fatal(dumpCmd.Err())
		}

		<-doneCh
	})

	t.Run("locked got nil, then acquire lock", func(t *testing.T) {
		keeper := newTestKeeper()
		key := "test-get-or-lock-but-nil"
		lockKey := "lock:" + key
		cmd := client.Set(lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetOrLock(key)
			if err != nil {
				t.Fatal(err)
			}
			if lock == nil {
				t.Fatal("should getting the new lock")
			}
			if res != nil {
				t.Fatal("result should be nil")
			}

			if err := lock.Release(); err != nil {
				t.Fatal("should not error")
			}
		}()

		// delete the lock key, but not set a value to the key
		delCmd := client.Del(lockKey)
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

		cmd := client.Set(lockKey, []byte("test"), 200*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		res, lock, err := keeper.GetOrLock(key)
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

	res, err := keeper.GetOrSet(key, func() (interface{}, error) {
		return []byte("test"), nil
	}, defaultTTL)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("result should not be nil")
	}

	// second call, getting data from cache
	res, err = keeper.GetOrSet(key, nil, defaultTTL)
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

	exists, err := keeper.CheckKeyExist(key)
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("should not exists")
	}

	cmd := client.Set(key, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	exists, err = keeper.CheckKeyExist(key)
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
	cmd := client.Set(key1, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	key2 := "test-delete-keys-2"
	cmd = client.Set(key2, []byte("test"), 500*time.Millisecond)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}

	err := keeper.DeleteByKeys([]string{key1, key2})
	if err != nil {
		t.Fatal(err)
	}

	exCmd := client.Exists(key1, key2)
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

	res, err := keeper.GetHashMember(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("should be nil")
	}

	bCmd := client.HSet(bucket, key, []byte("test"))
	if bCmd.Err() != nil {
		t.Fatal(bCmd)
	}

	res, err = keeper.GetHashMember(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("should not be nil")
	}
}

func TestKeeper_GetHashMemberOrLock(t *testing.T) {
	t.Run("getting the lock", func(t *testing.T) {
		keeper := newTestKeeper()
		bucket := "test-bucket-hash"
		key := "test-get-or-lock-hash"

		res, lock, err := keeper.GetHashMemberOrLock(bucket, key)
		if err != nil {
			t.Fatal(err)
		}
		if lock == nil {
			t.Fatal("should getting the lock")
		}
		if res != nil {
			t.Fatal("result should be nil")
		}

		err = lock.Release()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wait to getting lock", func(t *testing.T) {
		keeper := newTestKeeper()
		bucket := "test-bucket"
		key := "test-get-or-lockKey"
		lockKey := fmt.Sprintf("lockKey:%s:%s", bucket, key)

		cmd := client.Set(lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetHashMemberOrLock(bucket, key)
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

		hsetCmd := client.HSet(bucket, key, []byte("test"))
		if hsetCmd.Err() != nil {
			t.Fatal(hsetCmd.Err())
		}

		dumpCmd := client.Del(lockKey)
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

		cmd := client.Set(lockKey, []byte("test"), 500*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			res, lock, err := keeper.GetHashMemberOrLock(bucket, key)
			if err != nil {
				t.Fatal(err)
			}
			if lock == nil {
				t.Fatal("should getting the new lock")
			}
			if res != nil {
				t.Fatal("result should be nil")
			}

			if err := lock.Release(); err != nil {
				t.Fatal("should not error")
			}
		}()

		// delete the lock key, but not set a value to the key
		delCmd := client.Del(lockKey)
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

		cmd := client.Set(lockKey, []byte("test"), 200*time.Millisecond)
		if cmd.Err() != nil {
			t.Fatal(cmd.Err())
		}

		res, lock, err := keeper.GetHashMemberOrLock(bucket, key)
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

	hsetCmd := client.HSet(bucket, key1, []byte("test"))
	if hsetCmd.Err() != nil {
		t.Fatal(hsetCmd.Err())
	}

	hsetCmd = client.HSet(bucket, key2, []byte("test2"))
	if hsetCmd.Err() != nil {
		t.Fatal(hsetCmd.Err())
	}

	err := keeper.DeleteHashMember(bucket, key1)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.DeleteHashMember(bucket, key2)
	if err != nil {
		t.Fatal(err)
	}

	bCmd := client.HGet(bucket, key2)
	if bCmd.Err() == nil || bCmd.Err() != redis.Nil {
		t.Fatal("should be error and nil error")
	}
}

func TestKeeper_Store(t *testing.T) {
	keeper := newTestKeeper()
	key := "test-store"

	bCmd := client.Exists(key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	lock, err := keeper.AcquireLock(key)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.Store(lock, NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(key)
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

	bCmd := client.Exists(key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreWithoutBlocking(NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(key)
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

	bCmd := client.Exists(bucket)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreHashMember(bucket, NewItem(key, []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(bucket)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 1 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_StoreMultiWithoutBlocking(t *testing.T) {
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

	bCmd := client.Exists(item1.key, item2.key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreMultiWithoutBlocking([]Item{item1, item2})
	if err != nil {
		t.Fatal(err)
	}

	bCmd = client.Exists(item1.key, item2.key)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 2 {
		t.Fatal("should be 1")
	}
}

func TestKeeper_Persist(t *testing.T) {
	keeper := newTestKeeper()
	i := &item{
		key:   "tobe-persist",
		ttl:   defaultTTL,
		value: []byte("test-1"),
	}

	replyInt := client.Exists(i.key)
	if replyInt.Err() != nil {
		t.Fatal(replyInt.Err())
	}

	if replyInt.Val() != 0 {
		t.Fatal("should be 0")
	}

	err := keeper.StoreWithoutBlocking(i)
	if err != nil {
		t.Fatal(err)
	}

	err = keeper.Persist(i.key)
	if err != nil {
		t.Fatal(err)
	}

	replyDur := client.TTL(i.key)
	if replyDur.Err() != nil {
		t.Fatal(replyDur.Err())
	}

	if replyDur.Val() != -1*time.Second {
		t.Fatalf("expected: -1s, got: %d", replyDur.Val())
	}
}

func TestKeeper_GetMultiHashMembers(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		keeper := newTestKeeper()
		mapKeyToBucket := make(map[string]string)
		bucket := "bucket"
		var keys []string
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			keys = append(keys, key)
			mapKeyToBucket[key] = bucket
			err := keeper.StoreHashMember(bucket, &item{
				key:   key,
				ttl:   defaultTTL,
				value: []byte(key),
			})
			assert.NoError(t, err)
		}

		replies, err := keeper.GetMultiHashMembers(mapKeyToBucket)
		assert.NoError(t, err)

		// make sure all keys is found
		assert.Equal(t, len(mapKeyToBucket), len(replies))

		for _, reply := range replies {
			strCMD, ok := reply.(*redis.StringCmd)
			assert.True(t, ok)
			val, err := strCMD.Result()
			assert.NoError(t, err)
			assert.Contains(t, keys, val)
		}
	})

	t.Run("missing some keys", func(t *testing.T) {
		keeper := newTestKeeper()
		mapKeyToBucket := make(map[string]string)
		bucket := "bucket"
		var keys []string
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			keys = append(keys, key)
			err := keeper.StoreHashMember(bucket, &item{
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
			mapKeyToBucket[key] = bucket
		}

		replies, err := keeper.GetMultiHashMembers(mapKeyToBucket)
		assert.NoError(t, err)
		assert.Equal(t, len(mapKeyToBucket), len(replies))

		var notFoundArgs []string
		for _, reply := range replies {
			strCMD, ok := reply.(*redis.StringCmd)
			assert.True(t, ok)
			val, err := strCMD.Result()
			if val == "" {
				assert.EqualError(t, redis.Nil, err.Error())
				notFoundArgs = append(notFoundArgs, strCMD.Args()[2].(string))
				continue
			}
			assert.NoError(t, err)
			assert.Contains(t, keys, val)
		}

		for _, missingKey := range missingKeys {
			assert.Contains(t, notFoundArgs, missingKey)
		}
	})
}
