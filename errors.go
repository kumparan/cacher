package cacher

import "errors"

var (
	// ErrWaitTooLong :nodoc:
	ErrWaitTooLong = errors.New("wait too long")
	// ErrKeyNotExist :nodoc:
	ErrKeyNotExist = errors.New("key not exist")
	// ErrInvalidTTL error when the TTL value is not int64 type
	ErrInvalidTTL = errors.New("invalid TTL value")
)
