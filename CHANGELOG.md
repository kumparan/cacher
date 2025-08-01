# cacher

<a name="v1.20.0"></a>
## [v1.20.0] - 2025-07-31
### Other Improvements
- update keeper test
- update github workflows
- update keeper test to pass linter
- update golangci.yml
- bump dependencies to resolve security issues


<a name="v1.19.0"></a>
## [v1.19.0] - 2025-06-16
### New Features
- support INCRBY and DECRBY ([#58](https://github.com/kumparan/cacher/issues/58))


<a name="v1.18.0"></a>
## [v1.18.0] - 2025-05-13
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- fix get multi on disable caching
- fix disable cache on GetOrSet ([#46](https://github.com/kumparan/cacher/issues/46))
- only use json serializer ([#36](https://github.com/kumparan/cacher/issues/36))
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### Fixes
- handle nil keys
- enable get or set to handle struct ([#35](https://github.com/kumparan/cacher/issues/35))
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### New Features
- add StoreCachesWithCustomTTL ([#57](https://github.com/kumparan/cacher/issues/57))
- enable dynamic TTL on GetMultipleOrLock ([#53](https://github.com/kumparan/cacher/issues/53))
- add StoreCaches
- add ParseCacheResultToPointerObject ([#51](https://github.com/kumparan/cacher/issues/51))
- get multi hashmembers or lock
- get multiple or lock
- enable dynamic cache ttl ([#44](https://github.com/kumparan/cacher/issues/44))
- add github action to run unit test
- add DeleteHashMember on failover keeper
- delete and store nil with failover
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Other Improvements
- add readme
- close failover conn
- remove comment

### Test Improvements
- update tests


<a name="v2.7.2"></a>
## [v2.7.2] - 2024-01-05
### Code Improvements
- refactor v2, update go 1.18 and goredis v8

### Fixes
- fix acquirelock on GetHashMemberOrLock and upgrade go 1.20 ([#54](https://github.com/kumparan/cacher/issues/54))
- remove close pipeline ([#50](https://github.com/kumparan/cacher/issues/50))
- fix get multi hashmembers ([#47](https://github.com/kumparan/cacher/issues/47))

### New Features
- store multi hash member ([#42](https://github.com/kumparan/cacher/issues/42))
- upgrade to support Redis 7 ([#45](https://github.com/kumparan/cacher/issues/45))
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.17.0"></a>
## [v1.17.0] - 2023-06-05
### New Features
- enable dynamic TTL on GetMultipleOrLock ([#53](https://github.com/kumparan/cacher/issues/53))


<a name="v1.16.0"></a>
## [v1.16.0] - 2023-05-16
### New Features
- add StoreCaches


<a name="v1.15.0"></a>
## [v1.15.0] - 2023-05-10
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- fix get multi on disable caching
- fix disable cache on GetOrSet ([#46](https://github.com/kumparan/cacher/issues/46))
- only use json serializer ([#36](https://github.com/kumparan/cacher/issues/36))
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### Fixes
- handle nil keys
- enable get or set to handle struct ([#35](https://github.com/kumparan/cacher/issues/35))
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### New Features
- add ParseCacheResultToPointerObject ([#51](https://github.com/kumparan/cacher/issues/51))
- get multi hashmembers or lock
- get multiple or lock
- enable dynamic cache ttl ([#44](https://github.com/kumparan/cacher/issues/44))
- add github action to run unit test
- add DeleteHashMember on failover keeper
- delete and store nil with failover
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Other Improvements
- add readme
- close failover conn
- remove comment

### Test Improvements
- update tests


<a name="v2.7.1"></a>
## [v2.7.1] - 2023-04-11
### Fixes
- remove close pipeline ([#50](https://github.com/kumparan/cacher/issues/50))


<a name="v2.7.0"></a>
## [v2.7.0] - 2023-04-11
### Code Improvements
- refactor v2, update go 1.18 and goredis v8

### Fixes
- fix get multi hashmembers ([#47](https://github.com/kumparan/cacher/issues/47))

### New Features
- store multi hash member ([#42](https://github.com/kumparan/cacher/issues/42))
- upgrade to support Redis 7 ([#45](https://github.com/kumparan/cacher/issues/45))
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.14.1"></a>
## [v1.14.1] - 2023-04-10
### Fixes
- fix get multi on disable caching


<a name="v1.14.0"></a>
## [v1.14.0] - 2023-04-06
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- fix disable cache on GetOrSet ([#46](https://github.com/kumparan/cacher/issues/46))
- only use json serializer ([#36](https://github.com/kumparan/cacher/issues/36))
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### Fixes
- handle nil keys
- enable get or set to handle struct ([#35](https://github.com/kumparan/cacher/issues/35))
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### New Features
- get multi hashmembers or lock
- get multiple or lock
- enable dynamic cache ttl ([#44](https://github.com/kumparan/cacher/issues/44))
- add github action to run unit test
- add DeleteHashMember on failover keeper
- delete and store nil with failover
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Other Improvements
- add readme
- close failover conn
- remove comment

### Test Improvements
- update tests


<a name="v2.6.1"></a>
## [v2.6.1] - 2023-03-30
### Code Improvements
- refactor v2, update go 1.18 and goredis v8

### Fixes
- fix get multi hashmembers ([#47](https://github.com/kumparan/cacher/issues/47))

### New Features
- upgrade to support Redis 7 ([#45](https://github.com/kumparan/cacher/issues/45))
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.13.1"></a>
## [v1.13.1] - 2023-03-13
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- fix disable cache on GetOrSet ([#46](https://github.com/kumparan/cacher/issues/46))
- only use json serializer ([#36](https://github.com/kumparan/cacher/issues/36))
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### Fixes
- handle nil keys
- enable get or set to handle struct ([#35](https://github.com/kumparan/cacher/issues/35))
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### New Features
- enable dynamic cache ttl ([#44](https://github.com/kumparan/cacher/issues/44))
- add github action to run unit test
- add DeleteHashMember on failover keeper
- delete and store nil with failover
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Other Improvements
- add readme
- close failover conn
- remove comment

### Test Improvements
- update tests


<a name="v2.6.0"></a>
## [v2.6.0] - 2023-02-22
### Code Improvements
- refactor v2, update go 1.18 and goredis v8

### New Features
- upgrade to support Redis 7 ([#45](https://github.com/kumparan/cacher/issues/45))
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.13.0"></a>
## [v1.13.0] - 2023-02-15
### New Features
- enable dynamic cache ttl ([#44](https://github.com/kumparan/cacher/issues/44))


<a name="v1.12.2"></a>
## [v1.12.2] - 2022-12-14
### New Features
- add github action to run unit test


<a name="v1.12.1"></a>
## [v1.12.1] - 2022-10-27
### Fixes
- handle nil keys


<a name="v1.12.0"></a>
## [v1.12.0] - 2022-09-13
### New Features
- add DeleteHashMember on failover keeper
- delete and store nil with failover

### Other Improvements
- add readme
- close failover conn
- remove comment


<a name="v1.11.1"></a>
## [v1.11.1] - 2022-07-26
### Fixes
- only use json serializer ([#36](https://github.com/kumparan/cacher/issues/36))


<a name="v1.11.0"></a>
## [v1.11.0] - 2022-07-21
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- enable get or set to handle struct ([#35](https://github.com/kumparan/cacher/issues/35))
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### Fixes
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### New Features
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Test Improvements
- update tests


<a name="v2.5.0"></a>
## [v2.5.0] - 2022-07-11
### Code Improvements
- refactor v2, update go 1.18 and goredis v8

### New Features
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.10.1"></a>
## [v1.10.1] - 2022-06-29

<a name="v1.10.0"></a>
## [v1.10.0] - 2022-06-29
### Code Improvements
- update go.mod libraries
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))

### Fixes
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### New Features
- failover cache decorator ([#32](https://github.com/kumparan/cacher/issues/32))
- add SafeUnlock
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Test Improvements
- update tests


<a name="v2.4.0"></a>
## [v2.4.0] - 2022-03-23
### New Features
- add SafeUnlock to V2 ([#31](https://github.com/kumparan/cacher/issues/31))


<a name="v2.3.0"></a>
## [v2.3.0] - 2021-02-02
### New Features
- add GetMultiHashMember using redis pipeline ([#30](https://github.com/kumparan/cacher/issues/30))
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.9.3"></a>
## [v1.9.3] - 2021-01-20
### Code Improvements
- update go.mod libraries


<a name="v1.9.2"></a>
## [v1.9.2] - 2020-07-28
### New Features
- add SafeUnlock


<a name="v1.9.1"></a>
## [v1.9.1] - 2020-06-24
### Fixes
- panic on casting nil interface ([#26](https://github.com/kumparan/cacher/issues/26))


<a name="v1.9.0"></a>
## [v1.9.0] - 2020-06-03
### Code Improvements
- rename rep to reply
- using redigo package alias & add unit test
- check mutex nil or not before unlock it

### Fixes
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### New Features
- change HashScan result to a map
- add GetHashMemberThenDelete & HashScan
- add GetHashKeys
- add IncreaseHashMemberValue
- add StoreHashNilMember
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))

### Test Improvements
- update tests


<a name="v2.2.0"></a>
## [v2.2.0] - 2020-06-02
### New Features
- add persist implementation ([#25](https://github.com/kumparan/cacher/issues/25))
- lock on empty cache
- add test on v2


<a name="v1.8.4"></a>
## [v1.8.4] - 2020-04-22
### Code Improvements
- check mutex nil or not before unlock it


<a name="v1.8.3"></a>
## [v1.8.3] - 2020-04-21
### New Features
- add StoreHashNilMember


<a name="v1.8.2"></a>
## [v1.8.2] - 2020-04-21
### Fixes
- handle missing error checking on Get ([#21](https://github.com/kumparan/cacher/issues/21))


<a name="v1.8.1"></a>
## [v1.8.1] - 2020-04-20
### Fixes
- handle multiple locks on nil cache result ([#20](https://github.com/kumparan/cacher/issues/20))

### New Features
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))


<a name="v2.1.0"></a>
## [v2.1.0] - 2020-04-13
### New Features
- lock on empty cache


<a name="v2.0.0"></a>
## [v2.0.0] - 2020-04-09
### New Features
- add test on v2


<a name="v1.8.0"></a>
## [v1.8.0] - 2020-04-03
### New Features
- implement retry lock on empty cache on GetOrLock
- retry lock on empty cache


<a name="v1.7.2"></a>
## [v1.7.2] - 2020-03-23
### New Features
- remove garybird redis ([#17](https://github.com/kumparan/cacher/issues/17))


<a name="v2.0.0-pre1"></a>
## [v2.0.0-pre1] - 2020-01-21

<a name="v1.7.1"></a>
## [v1.7.1] - 2020-01-08
### Fixes
- fix different lock in GetHashMemberOrLock ([#14](https://github.com/kumparan/cacher/issues/14))


<a name="v1.7.0"></a>
## [v1.7.0] - 2019-10-02
### New Features
- add hash bucket ([#13](https://github.com/kumparan/cacher/issues/13))


<a name="v1.6.0"></a>
## [v1.6.0] - 2019-10-01

<a name="v1.5.5"></a>
## [v1.5.5] - 2019-09-27
### Code Improvements
- move offset function to internal ([#11](https://github.com/kumparan/cacher/issues/11))


<a name="v1.5.4"></a>
## [v1.5.4] - 2019-08-08

<a name="v1.5.3"></a>
## [v1.5.3] - 2019-05-07

<a name="v1.5.2"></a>
## [v1.5.2] - 2019-04-25

<a name="v1.5.1"></a>
## [v1.5.1] - 2019-04-23

<a name="v1.5.0"></a>
## [v1.5.0] - 2019-04-23

<a name="v1.4.0"></a>
## [v1.4.0] - 2019-04-16

<a name="v1.3.0"></a>
## [v1.3.0] - 2019-04-15

<a name="v1.2.1"></a>
## [v1.2.1] - 2019-04-10

<a name="v1.1.0"></a>
## [v1.1.0] - 2019-04-09

<a name="v1.2.0"></a>
## [v1.2.0] - 2019-04-09

<a name="v1.0.1"></a>
## [v1.0.1] - 2019-04-01

<a name="v1.0.0"></a>
## v1.0.0 - 2019-04-01

[Unreleased]: https://github.com/kumparan/cacher/compare/v1.20.0...HEAD
[v1.20.0]: https://github.com/kumparan/cacher/compare/v1.19.0...v1.20.0
[v1.19.0]: https://github.com/kumparan/cacher/compare/v1.18.0...v1.19.0
[v1.18.0]: https://github.com/kumparan/cacher/compare/v2.7.2...v1.18.0
[v2.7.2]: https://github.com/kumparan/cacher/compare/v1.17.0...v2.7.2
[v1.17.0]: https://github.com/kumparan/cacher/compare/v1.16.0...v1.17.0
[v1.16.0]: https://github.com/kumparan/cacher/compare/v1.15.0...v1.16.0
[v1.15.0]: https://github.com/kumparan/cacher/compare/v2.7.1...v1.15.0
[v2.7.1]: https://github.com/kumparan/cacher/compare/v2.7.0...v2.7.1
[v2.7.0]: https://github.com/kumparan/cacher/compare/v1.14.1...v2.7.0
[v1.14.1]: https://github.com/kumparan/cacher/compare/v1.14.0...v1.14.1
[v1.14.0]: https://github.com/kumparan/cacher/compare/v2.6.1...v1.14.0
[v2.6.1]: https://github.com/kumparan/cacher/compare/v1.13.1...v2.6.1
[v1.13.1]: https://github.com/kumparan/cacher/compare/v2.6.0...v1.13.1
[v2.6.0]: https://github.com/kumparan/cacher/compare/v1.13.0...v2.6.0
[v1.13.0]: https://github.com/kumparan/cacher/compare/v1.12.2...v1.13.0
[v1.12.2]: https://github.com/kumparan/cacher/compare/v1.12.1...v1.12.2
[v1.12.1]: https://github.com/kumparan/cacher/compare/v1.12.0...v1.12.1
[v1.12.0]: https://github.com/kumparan/cacher/compare/v1.11.1...v1.12.0
[v1.11.1]: https://github.com/kumparan/cacher/compare/v1.11.0...v1.11.1
[v1.11.0]: https://github.com/kumparan/cacher/compare/v2.5.0...v1.11.0
[v2.5.0]: https://github.com/kumparan/cacher/compare/v1.10.1...v2.5.0
[v1.10.1]: https://github.com/kumparan/cacher/compare/v1.10.0...v1.10.1
[v1.10.0]: https://github.com/kumparan/cacher/compare/v2.4.0...v1.10.0
[v2.4.0]: https://github.com/kumparan/cacher/compare/v2.3.0...v2.4.0
[v2.3.0]: https://github.com/kumparan/cacher/compare/v1.9.3...v2.3.0
[v1.9.3]: https://github.com/kumparan/cacher/compare/v1.9.2...v1.9.3
[v1.9.2]: https://github.com/kumparan/cacher/compare/v1.9.1...v1.9.2
[v1.9.1]: https://github.com/kumparan/cacher/compare/v1.9.0...v1.9.1
[v1.9.0]: https://github.com/kumparan/cacher/compare/v2.2.0...v1.9.0
[v2.2.0]: https://github.com/kumparan/cacher/compare/v1.8.4...v2.2.0
[v1.8.4]: https://github.com/kumparan/cacher/compare/v1.8.3...v1.8.4
[v1.8.3]: https://github.com/kumparan/cacher/compare/v1.8.2...v1.8.3
[v1.8.2]: https://github.com/kumparan/cacher/compare/v1.8.1...v1.8.2
[v1.8.1]: https://github.com/kumparan/cacher/compare/v2.1.0...v1.8.1
[v2.1.0]: https://github.com/kumparan/cacher/compare/v2.0.0...v2.1.0
[v2.0.0]: https://github.com/kumparan/cacher/compare/v1.8.0...v2.0.0
[v1.8.0]: https://github.com/kumparan/cacher/compare/v1.7.2...v1.8.0
[v1.7.2]: https://github.com/kumparan/cacher/compare/v2.0.0-pre1...v1.7.2
[v2.0.0-pre1]: https://github.com/kumparan/cacher/compare/v1.7.1...v2.0.0-pre1
[v1.7.1]: https://github.com/kumparan/cacher/compare/v1.7.0...v1.7.1
[v1.7.0]: https://github.com/kumparan/cacher/compare/v1.6.0...v1.7.0
[v1.6.0]: https://github.com/kumparan/cacher/compare/v1.5.5...v1.6.0
[v1.5.5]: https://github.com/kumparan/cacher/compare/v1.5.4...v1.5.5
[v1.5.4]: https://github.com/kumparan/cacher/compare/v1.5.3...v1.5.4
[v1.5.3]: https://github.com/kumparan/cacher/compare/v1.5.2...v1.5.3
[v1.5.2]: https://github.com/kumparan/cacher/compare/v1.5.1...v1.5.2
[v1.5.1]: https://github.com/kumparan/cacher/compare/v1.5.0...v1.5.1
[v1.5.0]: https://github.com/kumparan/cacher/compare/v1.4.0...v1.5.0
[v1.4.0]: https://github.com/kumparan/cacher/compare/v1.3.0...v1.4.0
[v1.3.0]: https://github.com/kumparan/cacher/compare/v1.2.1...v1.3.0
[v1.2.1]: https://github.com/kumparan/cacher/compare/v1.1.0...v1.2.1
[v1.1.0]: https://github.com/kumparan/cacher/compare/v1.2.0...v1.1.0
[v1.2.0]: https://github.com/kumparan/cacher/compare/v1.0.1...v1.2.0
[v1.0.1]: https://github.com/kumparan/cacher/compare/v1.0.0...v1.0.1
