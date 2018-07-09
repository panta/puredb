package puredb

import (
	"github.com/dgraph-io/badger"
	"fmt"
	"encoding/binary"
	"log"
)


type BucketCallback func(bucket *Bucket, k interface{}, v interface{}) error

type BucketPredicate func(bucket *Bucket, k interface{}, v interface{}) (bool, error)

type BucketOpts struct {
	PreAddFn         BucketCallback
}

//type BucketInterface interface {
//	Setup(db *PureDB, name string, opts BucketOpts) error
//	Cleanup()
//
//	GetName() string
//	GetOpts() *BucketOpts
//
//	MarshalKey(v interface{}) ([]byte, error)
//	UnmarshalKey(data []byte, v *interface{}) error
//	MarshalValue(v interface{}) ([]byte, error)
//	UnmarshalValue(data []byte, v *interface{}) error
//
//	Add(v interface{}) (int64, error)
//	Set(k interface{}, v interface{}) error
//	Get(k interface{}) (interface{}, error)
//	//Has(k interface{}) (bool, error)
//	Delete(k interface{}) error
//	//DeleteWith(p BucketPredicate) (int, error)
//	Pop(last bool) (interface{}, interface{}, error)
//
//	Iterate(fn BucketCallback) error
//	First() (interface{}, interface{}, error)
//	Last() (interface{}, interface{}, error)
//	Search(v interface{}, fn BucketCallback) (interface{}, error)
//	SearchOne(v interface{}, cmpFn BucketPredicate, reverse bool) (interface{}, interface{}, error)
//	SearchAll(v interface{}, cmpFn BucketPredicate, reverse bool) ([]interface{}, []interface{}, error)
//
//	//FirstWith(p BucketPredicate) (interface{}, interface{}, error)
//	//LastWith(p BucketPredicate) (interface{}, interface{}, error)
//
//	// Count
//	// CountWith
//	// Empty
//}

type Bucket struct {
	DB *PureDB
	badgerDB *badger.DB

	Name string
	Opts BucketOpts
	Seq  *badger.Sequence
}

func (bucket *Bucket) Badger() *badger.DB {
	return bucket.badgerDB
}

func (bucket *Bucket) Setup(db *PureDB, name string, opts BucketOpts) error {
	bucket.DB = db
	bucket.badgerDB = db.DB
	bucket.Name = name
	bucket.Opts = opts

	seq, err := bucket.badgerDB.GetSequence([]byte(bucket.Name), 100)
	bucket.Seq = seq

	return err
}

func (bucket *Bucket) Cleanup() {
	bucket.Seq.Release()
	bucket.Seq = nil
}

func (bucket *Bucket) GetName() string {
	return bucket.Name
}

func (bucket *Bucket) GetOpts() *BucketOpts {
	return &bucket.Opts
}

func (bucket *Bucket) Add(v interface{}) (int64, error) {
	db := bucket.badgerDB

	var id uint64

	err := db.Update(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		num, err := bucket.Seq.Next()
		if err != nil {
			return err
		}
		k_b := u64tob(num)

		id = num

		if bucket.Opts.PreAddFn != nil {
			err := bucket.Opts.PreAddFn(bucket, int64(num), v)
			if err != nil {
				return err
			}
		}

		v_b, err := Marshal(v)
		if err != nil {
			return err
		}
		k_prefixed := append(prefix, k_b...)
		return txn.Set(k_prefixed, v_b)
	})

	return int64(id), err
}

func (bucket *Bucket) Set(k interface{}, v interface{}) error {
	db := bucket.badgerDB

	err := db.Update(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))
		k_b, err := Marshal(k)
		if err != nil {
			return err
		}
		v_b, err := Marshal(v)
		if err != nil {
			return err
		}
		k_prefixed := append(prefix, k_b...)
		return txn.Set(k_prefixed, v_b)
	})

	return err
}

func (bucket *Bucket) Get(k interface{}, v interface{}) error {
	db := bucket.badgerDB

	k_b, err := Marshal(k)
	if err != nil {
		return err
	}

	err = db.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))
		k_prefixed := append(prefix, k_b...)
		item, err := txn.Get(k_prefixed)
		if err != nil {
			return err
		}
		v_b, err := item.Value()
		if err != nil {
			return err
		}

		err = Unmarshal(v_b, v)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (bucket *Bucket) Delete(k interface{}) error {
	db := bucket.badgerDB

	k_b, err := Marshal(k)
	if err != nil {
		return err
	}

	err = db.Update(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))
		k_prefixed := append(prefix, k_b...)
		return txn.Delete(k_prefixed)
	})

	return err
}

func (bucket *Bucket) Pop(last bool) (interface{}, interface{}, error) {
	db := bucket.badgerDB

	var k interface{}
	var v interface{}

	err := db.Update(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1
		opts.Reverse = last
		it := txn.NewIterator(opts)
		defer it.Close()

		if last {
			prefix = prefixBeyondEnd(prefix)
		}

		it.Seek(prefix)

		if (! it.ValidForPrefix(prefix)) {
			// empty set
			return fmt.Errorf("empty bucket")
		}

		item := it.Item()
		k_prefixed := item.Key()
		v_b, err := item.Value()
		if err != nil {
			return err
		}

		k_b := k_prefixed[len(prefix):]

		err = Unmarshal(k_b, &k)
		if err != nil {
			return err
		}
		err = Unmarshal(v_b, &v)
		if err != nil {
			return err
		}

		return txn.Delete(k_prefixed)
	})

	return k, v, err
}

func (bucket *Bucket) Iterate(fn BucketCallback) error {
	db := bucket.badgerDB

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k_prefixed := item.Key()
			v_b, err := item.Value()
			if err != nil {
				return err
			}

			k_b := k_prefixed[len(prefix):]

			var k_i interface{}
			var v_i interface{}
			err = Unmarshal(k_b, &k_i)
			if err != nil {
				return err
			}
			err = Unmarshal(v_b, &v_i)
			if err != nil {
				return err
			}
			err = fn(bucket, k_i, v_i)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (bucket *Bucket) First() (interface{}, interface{}, error) {
	db := bucket.badgerDB

	var first_k interface{}
	var first_v interface{}

	err := db.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefix)

		if (! it.ValidForPrefix(prefix)) {
			// empty set
			return fmt.Errorf("empty bucket")
		}

		item := it.Item()
		k_prefixed := item.Key()
		v_b, err := item.Value()
		if err != nil {
			return err
		}

		k_b := k_prefixed[len(prefix):]

		err = Unmarshal(k_b, &first_k)
		if err != nil {
			return err
		}
		err = Unmarshal(v_b, &first_v)
		if err != nil {
			return err
		}

		return nil
	})

	return first_k, first_v, err
}

func (bucket *Bucket) Last() (interface{}, interface{}, error) {
	db := bucket.badgerDB

	var last_k interface{}
	var last_v interface{}

	err := db.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefixBeyondEnd(prefix))

		if (! it.ValidForPrefix(prefix)) {
			// empty set
			return fmt.Errorf("empty bucket")
		}

		item := it.Item()
		k_prefixed := item.Key()
		v_b, err := item.Value()
		if err != nil {
			return err
		}

		k_b := k_prefixed[len(prefix):]

		err = Unmarshal(k_b, &last_k)
		if err != nil {
			return err
		}
		err = Unmarshal(v_b, &last_v)
		if err != nil {
			return err
		}

		return nil
	})

	return last_k, last_v, err
}

func (bucket *Bucket) Search(v interface{}, fn BucketCallback) (interface{}, error) {
	db := bucket.badgerDB

	var found_at interface{}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k_prefixed := item.Key()
			v_b, err := item.Value()
			if err != nil {
				return err
			}

			k_b := k_prefixed[len(prefix):]

			var k_i interface{}
			var v_i interface{}
			err = Unmarshal(k_b, &k_i)
			if err != nil {
				return err
			}
			err = Unmarshal(v_b, &v_i)
			if err != nil {
				return err
			}

			if v == v_i {
				found_at = k_i
				if fn != nil {
					err = fn(bucket, k_i, v_i)
				}
			}
		}
		return nil
	})

	return found_at, err
}

//	SearchOne(cmpFn BucketPredicate, reverse bool) (v interface{}, interface{}, interface{}, error)
//	SearchAll(cmpFn BucketPredicate, reverse bool) ([]interface{}, []interface{}, error)

func (bucket *Bucket) SearchOne(v interface{}, cmpFn BucketPredicate, reverse bool) (interface{}, interface{}, error) {
	db := bucket.badgerDB

	var found_k interface{}
	var found_v interface{}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		if reverse {
			prefix = prefixBeyondEnd(prefix)
		}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k_prefixed := item.Key()
			v_b, err := item.Value()
			if err != nil {
				return err
			}

			k_b := k_prefixed[len(prefix):]

			var k_i interface{}
			var v_i interface{}
			err = Unmarshal(k_b, &k_i)
			if err != nil {
				return err
			}
			err = Unmarshal(v_b, &v_i)
			if err != nil {
				return err
			}

			if cmpFn != nil {
				found, err := cmpFn(bucket, k_i, v_i)
				if err != nil {
					log.Printf("Bucket.SearchOne - error in cmpFn - k:%v v:%v err:%v", k_i, v_i, err)
					return err
				}
				if found {
					found_k = k_i
					found_v = v_i
					break
				}
			} else {
				if v == v_i {
					found_k = k_i
					found_v = v_i
					break
				}
			}
		}
		return nil
	})

	return found_k, found_v, err
}

func (bucket *Bucket) SearchAll(v interface{}, cmpFn BucketPredicate, reverse bool) ([]interface{}, []interface{}, error) {
	db := bucket.badgerDB

	var found_k []interface{}
	var found_v []interface{}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		if reverse {
			prefix = prefixBeyondEnd(prefix)
		}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k_prefixed := item.Key()
			v_b, err := item.Value()
			if err != nil {
				return err
			}

			k_b := k_prefixed[len(prefix):]

			var k_i interface{}
			var v_i interface{}
			err = Unmarshal(k_b, &k_i)
			if err != nil {
				return err
			}
			err = Unmarshal(v_b, &v_i)
			if err != nil {
				return err
			}

			if cmpFn != nil {
				found, err := cmpFn(bucket, k_i, v_i)
				if err != nil {
					log.Printf("Bucket.SearchAll - error in cmpFn k:%v v:%v err:%v", k_i, v_i, err)
					return err
				}
				if found {
					found_k = append(found_k, k_i)
					found_v = append(found_v, v_i)
				}
			} else {
				if v == v_i {
					found_k = append(found_k, k_i)
					found_v = append(found_v, v_i)
				}
			}
		}
		return nil
	})

	return found_k, found_v, err
}

//	// Count
//	// CountWith
//	// Empty

func (bucket *Bucket) Count() (int, error) {
	db := bucket.badgerDB

	count := 0

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false				// key-only iteration
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		it.Seek(prefix)
		for it.ValidForPrefix(prefix) {
			count++
			it.Next()
		}
		return nil
	})

	return count, err
}

func (bucket *Bucket) Empty() (bool, error) {
	db := bucket.badgerDB

	empty := true

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false				// key-only iteration
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))

		it.Seek(prefix)
		empty = (! it.ValidForPrefix(prefix))
		return nil
	})

	return empty, err
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// i64tob returns an 8-byte big endian representation of v.
func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// u64tob returns an 8-byte big endian representation of v.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func prefixBeyondEnd(prefix []byte) []byte {
	return append(prefix, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}...)		// trick, see https://github.com/dgraph-io/badger/issues/436#issuecomment-400095559
}
