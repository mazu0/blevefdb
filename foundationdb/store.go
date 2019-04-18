package foundationdb

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const (
	// Name is the name of this KVStore that is registered in bleve store registry
	Name = "foundationdb"

	// prefix for []byte keys (tuple packing)
	byteprefix = 0x01
)

// Store is a FoundationDB implementation of bleve KVStore interface
type Store struct {
	db  *fdb.Database
	mo  store.MergeOperator
	sub subspace.Subspace
}

// New returns a new Store for interacting with FoundationDB
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	// set API version if key is set in the config
	if apiVersion, exists := config["apiVersion"]; exists {
		err := fdb.APIVersion(apiVersion.(int))
		if err != nil {
			return nil, err
		}
	}

	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	// get foundationdb config
	var sub subspace.Subspace
	var buf *bytes.Buffer
	if cDir, exists := config["directory"]; exists {
		dir, err := directory.CreateOrOpen(db, []string{cDir.(string)}, nil)
		if err != nil {
			return nil, err
		}

		subspace := config["subspace"].(string)
		sub = dir.Sub(subspace)
	}

	if err != nil {
		return nil, err
	}

	return &Store{
		mo:        mo,
		db:        &db,
		sub:       sub,
		prefixBuf: buf,
	}, nil
}

// Writer returns a KVWriter which is used for writting data to FDB
func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: s,
	}, nil
}

// Reader returns a KVReader which is used for reading data from FDB
func (s *Store) Reader() (store.KVReader, error) {
	return &Reader{
		db:    s.db,
		store: s,
	}, nil
}

// Close closes the KVStore
func (s *Store) Close() error {
	return nil
}

func (s *Store) formatKey(key []byte) fdb.Key {
	if s.sub == nil {
		return fdb.Key(key)
	}

	return s.sub.Pack(tuple.Tuple{key})
}

func (s *Store) getPrefixRange(key []byte) (keyRange fdb.KeyRange, err error) {
	if s.sub == nil {
		return fdb.PrefixRange(key)
	}

	return fdb.KeyRange{
		Begin: s.sub.Pack(tuple.Tuple{concat(key, 0x00)}),
		End:   s.sub.Pack(tuple.Tuple{concat(key, 0xFF)}),
	}, nil
}

// Register KVStore to bleve store registry
func init() {
	registry.RegisterKVStore(Name, New)
}

func concat(a []byte, b ...byte) []byte {
	r := make([]byte, len(a)+len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}
