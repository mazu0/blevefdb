package foundationdb

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/blevesearch/bleve/index/store"
)

var ErrTxTimeout = "FoundationDB error code 1007 (Transaction is too old to perform reads or be committed)"

// Iterator is a foundationDB implementation of bleve KVIterator interface
type Iterator struct {
	store    *Store
	db       *fdb.Database
	tx       fdb.Transaction
	iterator *fdb.RangeIterator
	curr     *fdb.KeyValue
	done     bool
	err      error
	keyRange fdb.KeyRange
}

func newIterator(store *Store, db *fdb.Database, keyRange fdb.KeyRange) store.KVIterator {
	tx, iter, err := createFdbTxAndIterator(db, keyRange)
	if err != nil {
		return &Iterator{
			err: err,
		}
	}

	it := &Iterator{
		store:    store,
		db:       db,
		tx:       tx,
		iterator: iter,
		keyRange: keyRange,
	}
	// the iterator must be set to first item
	it.Next()

	return it
}

// Seek will advance the iterator to the specified KeyValue
func (i *Iterator) Seek(key []byte) {
	for ; !i.done; i.Next() {
		if bytes.Compare(i.Key(), key) >= 0 {
			return
		}
	}
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
		// refresh transaction and set iterator to a new range (from current key to original range end)
		if ErrTxTimeout == err.Error() && i.curr != nil {
			i.tx.Cancel()
			i.tx, i.iterator, err = createFdbTxAndIterator(
				i.db,
				fdb.KeyRange{
					Begin: i.curr.Key,
					End:   i.keyRange.End,
				},
			)

			// error creating transaction
			if err != nil {
				i.curr = nil
				i.err = err
				return
			}

			i.Next()
			return
		}

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

	if i.store.sub == nil {
		return i.curr.Key
	}

	return i.store.unformatKey(i.curr.Key)
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

	if i.store.sub == nil {
		return i.curr.Key, i.curr.Value, true
	}

	return i.store.unformatKey(i.curr.Key), i.curr.Value, true
}

// Close closes the iterator
func (i *Iterator) Close() error {
	i.tx.Cancel()
	i.curr = nil
	i.done = true
	i.err = nil

	return nil
}

// Creates
// - new transaction for getting range iterator and for cancelling in Close
// - range iterator
func createFdbTxAndIterator(db *fdb.Database, keyRange fdb.KeyRange) (fdb.Transaction, *fdb.RangeIterator, error) {
	tx, err := db.CreateTransaction()
	if err != nil {
		return tx, nil, err
	}

	return tx, tx.GetRange(keyRange, fdb.RangeOptions{}).Iterator(), err
}
