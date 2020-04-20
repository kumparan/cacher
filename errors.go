package cacher

import "errors"

var (
	// ErrWaitTooLong :nodoc:
	ErrWaitTooLong = errors.New("wait too long")
	// ErrKeyNotExist :nodoc:
	ErrKeyNotExist = errors.New("key not exist")
)
