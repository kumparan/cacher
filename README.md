# cacher
cacher is a go redis client wrapper equipped with some utility function that make your life easier in using redis. cacher created based on [redigo](https://github.com/gomodule/redigo) library.
## usage
- Create Redis Connection
```go
redisConn := &redigo.Pool{
    MaxIdle:     100,
    MaxActive:   10000,
    IdleTimeout: 240 * time.Second,
    Dial: func() (redigo.Conn, error) {
        c, err := redigo.DialURL("redis://localhost:6379/0")
        if err != nil {
            return nil, err
        }
        return c, err
    },
}
redisLockConn := &redigo.Pool{
    MaxIdle:     100,
    MaxActive:   10000,
    IdleTimeout: 240 * time.Second,
    Dial: func() (redigo.Conn, error) {
        c, err := redigo.DialURL("redis://localhost:6379/0")
        if err != nil {
            return nil, err
        }
        return c, err
    },
}
```
- Create Keeper  
`Keeper` hold all the utility function while there are also `KeeperWithFailover` which has all the `Keeper` functionality but with some extension. `KeeperWithFailover`  will store cache to 2 redis db, one for main cache, another one for failover cache. The failover cache will keep updated like the main cache but with longer TTL. For more information about cache with failover, refer to [here](https://docs.microsoft.com/en-us/azure/azure-cache-for-redis/cache-failover).
```go
// keeper
cacheKeeper := cacher.NewKeeper()
cacheKeeper.SetConnectionPool(redisConn)
cacheKeeper.SetLockConnectionPool(redisLockConn)
cacheKeeper.SetDefaultTTL(15 * time.Minute)

// keeper with failover
cacheKeeperWithFailover := cacher.NewKeeperWithFailover()
cacheKeeperWithFailover.SetConnectionPool(redisConn)
cacheKeeperWithFailover.SetLockConnectionPool(redisLockConn)
cacheKeeperWithFailover.SetDefaultTTL(15 * time.Minute)
cacheKeeperWithFailover.SetFailoverConnectionPool(redisFailoverConn)
cacheKeeperWithFailover.SetFailoverTTL(1 * time.Hour)
```
- Using the keeper

**Store item to redis**
```go
cacheItem := cacher.NewItem("cache-key", "value")
cacheKeeper.StoreWithouBlocking(cacheItem)
```
**Store item with custom TTL**  
note that custom TTL doesn't work on `StoreFailover` method
```go
cacheItem := cacher.NewItemWithCustomTTL("cache-key", "value", time.Minute * 5)
cacheKeeper.StoreWithouBlocking(cacheItem)
```
**Get value from cache**
```go
res, _ := cacheKeeper.Get("cache-key")
```
**Get With Lock**
```go
res, mu, _ := cacheKeeper.GetOrLock("cache-key")
```
**Get or Set**  
`GetOrSet` will get value from redis, if there's no cache with the defined key, then it will execute the getter function you specified and store the returned value to redis.  
If `GetOrSet` used by `KeeperWithFailover`, it will return value from failover cache if there's no cache from the main cache and the getter function returned an error.
```go
getterFunction := func() (any, error) {
	// get data from data source
	return "data-from-db", nil
}
res, _ := cacheKeeper.GetOrSet(key, getterFunction)
```
**Delete**  
`DeleteByKeys` will delete main cache and also the failover cache if u use `KeeperWithFailover`
```go
keys := []string{"cache-a", "cache-b", "cache-c"}
err := cacheKeeper.DeleteByKeys(keys)
```
