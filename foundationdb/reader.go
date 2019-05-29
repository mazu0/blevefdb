package foundationdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/blevesearch/bleve/index/store"
)

// Reader is a FDB implementation of bleve KVReader interface
type Reader struct {
	db    *fdb.Database
	store *Store
}

// Get returns the value associated with the key
func (r *Reader) Get(key []byte) ([]byte, error) {
	val, err := r.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		return tr.Get(r.store.formatKey(key)).Get()
	})
	if err != nil {
		return nil, err
	}

	return val.([]byte), nil
}

// MultiGet retrieves multiple values in one call.
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

// PrefixIterator returns a KVIterator that iterates through all KeyValue's with the specified prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	prefixRange, err := r.store.getPrefixRange(prefix)
	if err != nil {
		return &Iterator{
			err: err,
		}
	}

	return newIterator(r.db, prefixRange)
}

// RangeIterator returns a KVIterator that iterates
// through all KeyValue's with key >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	return newIterator(r.db, fdb.KeyRange{
		Begin: r.store.formatKey(start),
		End:   r.store.formatKey(end),
	})
}

// Close closes the Reader
func (r *Reader) Close() error {
	return nil
}
