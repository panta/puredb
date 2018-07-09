package puredb

import (
	"github.com/dgraph-io/badger"
	"fmt"
)

type BucketIterOpts struct {
	Prefix		[]byte
	Reverse		bool
}
type BucketIter struct {
	bucket	*Bucket
	prefix	[]byte
	txn		*badger.Txn
	it		*badger.Iterator
	bOpts	*badger.IteratorOptions
	Opts	BucketIterOpts
	Err		error
}

func NewBucketIter(bucket *Bucket, opts BucketIterOpts) *BucketIter {
	bOpts := badger.DefaultIteratorOptions
	bOpts.PrefetchSize = 10
	bOpts.Reverse = opts.Reverse

	db := bucket.badgerDB

	txn := db.NewTransaction(false)		// read-only transaction (update set to false)

	prefix := []byte(fmt.Sprintf("%s__", bucket.GetName()))
	if len(opts.Prefix) > 0 {
		prefix = append(prefix, opts.Prefix...)
	}
	if opts.Reverse {
		prefix = append(prefix, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}...)		// trick, see https://github.com/dgraph-io/badger/issues/436#issuecomment-400095559
	}

	it := BucketIter{
		bucket: bucket,
		prefix: prefix,
		txn: txn,
		it: txn.NewIterator(bOpts),
		bOpts: &bOpts,
		Opts: opts,
	}

	it.it.Seek(it.prefix)

	return &it
}

func (it *BucketIter) Close() {
	it.it.Close()
	it.txn.Discard()
}

func (it *BucketIter) Rewind() {
	it.it.Seek(it.prefix)
}

func (it *BucketIter) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *BucketIter) EOF() bool {
	return (! it.it.ValidForPrefix(it.prefix))
}

func (it *BucketIter) Next() {
	it.it.Next()
}

func (it *BucketIter) Error() bool {
	return it.Err != nil
}

func (it *BucketIter) Get(keyp interface{}, valuep interface{}) error {
	item := it.it.Item()
	k_prefixed := item.Key()
	v_b, err := item.Value()
	if err != nil {
		it.Err = err
		return err
	}

	k_b := k_prefixed[len(it.prefix):]

	err = Unmarshal(k_b, keyp)
	if err != nil {
		it.Err = err
		return err
	}
	err = Unmarshal(v_b, valuep)
	if err != nil {
		it.Err = err
		return err
	}

	return nil
}

func (it *BucketIter) Find(value interface{}, cmpFn BucketPredicate, keyp *interface{}) (bool, error) {
	for ; it.Valid(); it.Next() {
		item := it.it.Item()
		k_prefixed := item.Key()
		v_b, err := item.Value()
		if err != nil {
			it.Err = err
			return false, err
		}

		k_b := k_prefixed[len(it.prefix):]

		var k_i interface{}
		var v_i interface{}
		err = Unmarshal(k_b, &k_i)
		if err != nil {
			it.Err = err
			return false, err
		}
		err = Unmarshal(v_b, &v_i)
		if err != nil {
			it.Err = err
			return false, err
		}

		if cmpFn != nil {
			found, err := cmpFn(it.bucket, k_i, v_i)
			if err != nil {
				it.Err = err
				return false, err
			}
			if found {
				*keyp = k_i
				return true, nil
			}
		} else if v_i == value {
			*keyp = k_i
			return true, nil
		}
	}

	return false, nil
}
