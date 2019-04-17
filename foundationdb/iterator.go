package foundationdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/blevesearch/bleve/index/store"
)

// Iterator is a foundationDB implementation of bleve KVIterator interface
type Iterator struct {
	tx       fdb.Transaction
	iterator *fdb.RangeIterator
	curr     *fdb.KeyValue
	done     bool
	err      error
}

func newIterator(db *fdb.Database, keyRange fdb.KeyRange) store.KVIterator {
	tx, err := db.CreateTransaction()
	if err != nil {
		return &Iterator{
			err: err,
		}
	}

	it := &Iterator{
		tx:       tx,
		iterator: tx.GetRange(keyRange, fdb.RangeOptions{}).Iterator(),
	}
	// the iterator must be set to first item
	it.Next()

	return it
}

// Seek will advance the iterator to the specified KeyValue
func (i *Iterator) Seek(key []byte) {
	// TODO
	panic("Iterator Seek method not implemented")
}

// Next will advance the iterator to the next KeyValue if exists
func (i *Iterator) Next() {
	if !i.iterator.Advance() {
		i.curr = nil
		i.done = true
		return
	}

	kv, err := i.iterator.Get()
	if err != nil {
		i.curr = nil
		i.err = err
		return
	}
	i.curr = &kv
}

// Key returns the key of the KeyValue pointed to by the iterator
func (i *Iterator) Key() []byte {
	if i.curr == nil {
		return nil
	}

	return i.curr.Key
}

// Value returns the value of the KeyValue pointed to by the iterator
func (i *Iterator) Value() []byte {
	if i.curr == nil {
		return nil
	}

	return i.curr.Value
}

// Valid returns whether the iterator is in a valid state
func (i *Iterator) Valid() bool {
	if i.iterator == nil {
		return false
	}

	return !i.done && i.err == nil
}

// Current returns key and value of the KeyValue pointed to (if any)
// and a flag if the iterator is in valid state
func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.curr == nil || !i.Valid() {
		return nil, nil, false
	}

	return i.curr.Key, i.curr.Value, true
}

// Close closes the iterator
func (i *Iterator) Close() error {
	i.tx.Cancel()
	i.curr = nil
	i.done = true
	i.err = nil

	return nil
}
