package puredb

import (
	"github.com/dgraph-io/badger"
	"os"
	"log"
)

type PureDB struct {
	DB *badger.DB
	badgerOpts badger.Options
	Pathname string

	buckets buckets
	tables tables
}

// use "functional options"
// see:
//   - https://stackoverflow.com/a/26326418/1363486
//   - http://commandcenter.blogspot.com.au/2014/01/self-referential-functions-and-design.html
//   - http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type PureDBOptionFn func (*PureDB) error

func Open(pathname string, options ...PureDBOptionFn) (*PureDB, error) {
	opts := badger.DefaultOptions
	opts.SyncWrites = true
	opts.Dir = pathname
	opts.ValueDir = pathname

	pureDb := PureDB{
		badgerOpts: opts,
		Pathname: pathname,
	}
	for _, option := range options {
		err := option(&pureDb)
		if err != nil {
			return nil, err
		}
	}

	badgerDb, err := badger.Open(pureDb.badgerOpts)
	if err != nil {
		log.Printf("can't open/create DB pathname: %q err: %v", pureDb.badgerOpts.Dir, err)
		return nil, err
	}
	pureDb.DB = badgerDb

	pureDb.buckets.Init(&pureDb)
	pureDb.tables.Init(&pureDb)

	return &pureDb, nil
}

func (db *PureDB) Close() {
	db.tables.Cleanup()
	db.buckets.Cleanup()
	db.DB.Close()
}

func (db *PureDB) Destroy() {
	db.tables.Cleanup()
	db.buckets.Cleanup()
	db.DB.Close()
	os.RemoveAll(db.Pathname)
}

func (db *PureDB) Badger() *badger.DB {
	return db.DB
}

func (db *PureDB) AddBucket(name string, opts BucketOpts) (*Bucket, error) {
	return db.buckets.Add(name, opts)
}

func (db *PureDB) GetBucket(name string) *Bucket {
	return db.buckets.Get(name)
}

func (db *PureDB) AddTable(name string) (*Table, error) {
	return db.tables.Add(name)
}

func (db *PureDB) GetTable(name string) *Table {
	return db.tables.Get(name)
}

func (db *PureDB) NewReadOnlyTransaction() *Transaction {
	return NewReadOnlyTransaction(db)
}

func (db *PureDB) NewReadWriteTransaction() *Transaction {
	return NewReadWriteTransaction(db)
}

// View executes a function, automatically creating and managing a read-only transaction.
// Error returned by the function is returned by View.
func (db *PureDB) View(fn func(transaction *Transaction) error) error {
	transaction := NewReadOnlyTransaction(db)
	defer transaction.Discard()

	return fn(transaction)
}

// Update executes a function, automatically creating and managing a read-write transaction.
// Error returned by the function is returned by Update.
func (db *PureDB) Update(fn func(transaction *Transaction) error) error {
	transaction := NewReadWriteTransaction(db)
	defer transaction.Discard()

	if err := fn(transaction); err != nil {
		return err
	}

	return transaction.Commit()
}
