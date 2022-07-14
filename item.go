package cacher

import (
	"time"
)

type (
	// Item :nodoc:
	Item interface {
		GetTTLInt64() int64
		GetKey() string
		GetValue() any
		SetTTL(ttl time.Duration)
	}

	item struct {
		key   string
		value any
		ttl   time.Duration
	}
)

// WithTTL define custom TTL used in GetOrSet
func WithTTL(ttl time.Duration) func(Item) {
	return func(i Item) {
		i.SetTTL(ttl)
	}
}

// NewItem :nodoc:
func NewItem(key string, value any) Item {
	return &item{
		key:   key,
		value: value,
	}
}

// NewItemWithCustomTTL :nodoc:
func NewItemWithCustomTTL(key string, value any, customTTL time.Duration) Item {
	return &item{
		key:   key,
		value: value,
		ttl:   customTTL,
	}
}

// GetTTLInt64 :nodoc:
func (i *item) GetTTLInt64() int64 {
	return int64(i.ttl.Seconds())
}

// SetTTL set TTL
func (i *item) SetTTL(ttl time.Duration) {
	i.ttl = ttl
}

// GetKey :nodoc:
func (i *item) GetKey() string {
	return i.key
}

// GetValue :nodoc:
func (i *item) GetValue() any {
	return i.value
}
