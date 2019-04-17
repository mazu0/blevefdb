package foundationdb

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/blevesearch/bleve/index/store"
)

// Writer is a foundationDB implementation of bleve KVWriter interface
type Writer struct {
	store *Store
}

// NewBatch returns a KVBatch for performing batch operations
func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) error {
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("Error asserting batch")
	}

	// process merges
	_, err := w.store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for k, mergeOps := range emulatedBatch.Merger.Merges {
			kb := []byte(k)
			key := w.store.formatKey(kb)
			existingVal, e := tr.Get(key).Get()
			if e != nil {
				return nil, e
			}
			mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
			if !fullMergeOk {
				return nil, fmt.Errorf("Error executing FullMerge")
			}
			tr.Set(key, mergedVal)
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	// apply batch
	_, err = w.store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for _, op := range emulatedBatch.Ops {
			key := w.store.formatKey(op.K)
			if op.V != nil {
				tr.Set(key, op.V)
			} else {
				tr.Clear(key)
			}
		}
		return nil, nil
	})

	return err
}

// Close closes the writer
func (w *Writer) Close() error {
	return nil
}
