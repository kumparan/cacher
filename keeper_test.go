package cacher

import (
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
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
		key := "test-get-or-lock"
		lock := "lock:" + bucket + ":" + key

		cmd := client.Set(lock, []byte("test"), 500*time.Millisecond)
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

		dumpCmd := client.Del(lock)
		if dumpCmd.Err() != nil {
			t.Fatal(dumpCmd.Err())
		}

		<-doneCh
	})
}

func TestKeeper_DeleteHashMember(t *testing.T) {
	keeper := newTestKeeper()
	bucket := "test-bucket"
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

	bCmd := client.Exists(bucket)
	if bCmd.Err() != nil {
		t.Fatal(bCmd.Err())
	}

	if bCmd.Val() != 0 {
		t.Fatal("should be 0")
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
