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

	return &pureDb, nil
}

func (db *PureDB) Close() {
	db.buckets.Cleanup()
	db.DB.Close()
}

func (db *PureDB) Destroy() {
	db.buckets.Cleanup()
	db.DB.Close()
	os.RemoveAll(db.Pathname)
}

func (db *PureDB) Badger() *badger.DB {
	return db.DB
}

func (db *PureDB) AddBucket(name string, opts BucketOpts) error {
	log.Printf("PureDB::AddBucket - name:%v opts:%v", name, opts)
	return db.buckets.Add(name, opts)
}

func (db *PureDB) GetBucket(name string) *Bucket {
	return db.buckets.Get(name)
}
