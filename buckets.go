package puredb

import "log"

type buckets struct {
	DB	*PureDB
	Map	map[string]*Bucket
}

func (buckets *buckets) Init(db *PureDB) {
	buckets.DB = db
	buckets.Map = make(map[string]*Bucket)
}

func (buckets *buckets) Cleanup() {
	for _, ti := range buckets.Map {
		ti.Cleanup()
	}
}

func (buckets *buckets) Add(name string, opts BucketOpts) error {
	log.Printf("buckets::Add name:%v opts:%v", name, opts)
	bucket := Bucket{}
	err := bucket.Setup(buckets.DB, name, opts)
	if err != nil {
		return err
	}
	buckets.Map[name] = &bucket
	return nil
}

func (buckets *buckets) Get(name string) *Bucket {
	return buckets.Map[name]
}
