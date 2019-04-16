package cacher

import (
	"time"
)

type (
	// Item :nodoc:
	Item interface {
		GetTTLInt64() int64
		GetKey() string
		GetValue() interface{}
	}

	item struct {
		key   string
		value interface{}
		ttl   time.Duration
	}

	List interface {
		GetListName() string
		GetValue() interface{}
	}

	list struct {
		listName string
		value    interface{}
	}
)

// NewItem :nodoc:
func NewItem(key string, value interface{}) Item {
	return &item{
		key:   key,
		value: value,
	}
}

// NewItemWithCustomTTL :nodoc:
func NewItemWithCustomTTL(key string, value interface{}, customTTL time.Duration) Item {
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

// GetKey :nodoc:
func (i *item) GetKey() string {
	return i.key
}

// GetValue :nodoc:
func (i *item) GetValue() interface{} {
	return i.value
}

// NewList :nodoc:
func NewList(listName string, value interface{}) List {
	return &list{
		listName: listName,
		value:    value,
	}
}

// GetKey :nodoc:
func (i *list) GetListName() string {
	return i.listName
}

// GetValue :nodoc:
func (i *list) GetValue() interface{} {
	return i.value
}
