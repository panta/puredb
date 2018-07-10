package puredb

import (
	"testing"
	"os"
	"io/ioutil"
	"time"
	"github.com/vmihailenco/msgpack"
	"reflect"
	"log"
)

const (
	bucket_id_book = "id_book"
	bucket_published_id = "published_id"
)

type Book struct {
	Id			int64		`puredb:"primary,auto"`
	Author		string
	Title		string
	Year		int			`puredb:"index,unique"`
	Available	bool
	Price		float64
	Published	time.Time	`puredb:"index"`
}

func (book *Book) Marshal() ([]byte, error) {
	b := book
	b.Published = b.Published.UTC()
	return msgpack.Marshal(b)
}

func (book *Book) Unmarshal(data []byte) (error) {
	err := msgpack.Unmarshal(data, book)
	if err != nil {
		return err
	}
	book.Published = book.Published.UTC()
	return nil
}

func addBook(t *testing.T, db *PureDB, book *Book) error {
	id, err := db.GetBucket(bucket_id_book).Add(book)
	if err != nil {
		t.Fatalf("can't add record %v - err:%v", book, err)
		return err
	}
	err = db.GetBucket(bucket_published_id).Set(book.Published, id)
	if err != nil {
		t.Fatalf("can't add record %v (id %v) to %v bucket - err:%v", book, id, bucket_published_id, err)
		return err
	}

	retrieved_book := Book{}
	err = db.GetBucket(bucket_id_book).Get(id, &retrieved_book)
	if err != nil {
		t.Fatalf("can't get back record from id (%v) err:%v", id, err)
		return err
	}

	if id != retrieved_book.Id {
		t.Fatalf("wrong id in retrieved record (expected %v found %v)", id, retrieved_book.Id)
		return err
	}

	if !reflect.DeepEqual(*book, retrieved_book) {
		t.Fatal("retrieved record differs", book, retrieved_book)
		return err
	}

	id_i := int64(-1)
	err = db.GetBucket(bucket_published_id).Get(retrieved_book.Published, &id_i)
	if err != nil {
		t.Fatalf("can't get record from %v (published %v) - err:%v", bucket_published_id, retrieved_book.Published, err)
		return err
	}

	if id_i != id {
		t.Fatalf("id from %v (%v) differs from primary id (%v)", bucket_published_id, id_i, id)
		return err
	}

	return nil
}

func TestBuckets(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Destroy()

	id_book_opts := BucketOpts{}
	id_book_opts.PreAddFn = func (bucket *Bucket, k interface{}, v interface{}) error {
		id := k.(int64)
		lpr := v.(*Book)
		lpr.Id = id
		return nil
	}

	db.AddBucket(bucket_id_book, id_book_opts)
	db.AddBucket(bucket_published_id, BucketOpts{})
	//db.AddBucket(bucket_year_id, BucketOpts{})

	t1, err := time.Parse(time.RFC3339, "1623-01-01T10:00:00Z")
	panicOnErr(err)
	b1 := Book{
		Id: -1,
		Author: "William Shakespeare",
		Title: "Much Ado About Nothing",
		Year: 1623,
		Available: true,
		Price: 12.34,
		Published: t1,
	}
	err = addBook(t, db, &b1)
	panicOnErr(err)

	t2, err := time.Parse(time.RFC3339, "1719-01-01T10:00:00Z")
	panicOnErr(err)
	b2 := Book{
		Id: -1,
		Author: "Daniel Defoe",
		Title: "Robinson Crusoe",
		Year: 1719,
		Available: true,
		Price: 17.99,
		Published: t2,
	}
	err = addBook(t, db, &b2)
	panicOnErr(err)

	t3, err := time.Parse(time.RFC3339, "1500-01-01T10:00:00Z")
	panicOnErr(err)
	b3 := Book{
		Id: -1,
		Author: "AAAA AAAA",
		Title: "AAAA",
		Year: 1500,
		Available: true,
		Price: 1.99,
		Published: t3,
	}
	err = addBook(t, db, &b3)
	panicOnErr(err)

	t4, err := time.Parse(time.RFC3339, "1200-01-01T10:00:00Z")
	panicOnErr(err)
	b4 := Book{
		Id: -1,
		Author: "BBBB BBBB",
		Title: "BBBB",
		Year: 1200,
		Available: true,
		Price: 1.99,
		Published: t4,
	}
	err = addBook(t, db, &b4)
	panicOnErr(err)

	t5, err := time.Parse(time.RFC3339, "1200-01-01T09:00:00Z")
	panicOnErr(err)
	b5 := Book{
		Id: -1,
		Author: "CCCC CCCC",
		Title: "CCCC",
		Year: 1200,
		Available: true,
		Price: 1.99,
		Published: t5,
	}
	err = addBook(t, db, &b5)
	panicOnErr(err)

	it := NewBucketIter(db.GetBucket(bucket_published_id), BucketIterOpts{})
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		key := time.Time{}
		id := int64(-1)
		err = it.Get(&key, &id)
		if err != nil {
			t.Fatalf("in iteration on %v - err:%v", bucket_published_id, err)
		}
		log.Printf("%v [%v]  key:%v  id:%v", bucket_published_id, i, key, id)
		i++
	}
}

func TestTables(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Destroy()

	bookTable, err := db.AddTable("books")
	if err != nil {
		t.Fatalf("adding table - err:%v", err)
	}

	t1, err := time.Parse(time.RFC3339, "1623-01-01T10:00:00Z")
	panicOnErr(err)
	b1 := Book{
		Id: -1,
		Author: "William Shakespeare",
		Title: "Much Ado About Nothing",
		Year: 1623,
		Available: true,
		Price: 12.34,
		Published: t1,
	}

	_, err = bookTable.Save(&b1)
	if err != nil {
		t.Fatalf("adding record - err:%v", err)
	}

	_, err = bookTable.Save(&b1)
	if err != nil {
		if _, ok := err.(*DuplicateKeyError); !ok {
			t.Fatalf("adding duplicate - err:%v", err)
		} else {
			log.Printf("correctly received integrity error on duplicate\n")
		}
	} else {
		t.Fatalf("no error when adding duplicate, expected integrity error")
	}

	//t2, err := time.Parse(time.RFC3339, "1719-01-01T10:00:00Z")
	//panicOnErr(err)
	//b2 := Book{
	//	Id: -1,
	//	Author: "Daniel Defoe",
	//	Title: "Robinson Crusoe",
	//	Year: 1719,
	//	Available: true,
	//	Price: 17.99,
	//	Published: t2,
	//}
	//
	//t3, err := time.Parse(time.RFC3339, "1500-01-01T10:00:00Z")
	//panicOnErr(err)
	//b3 := Book{
	//	Id: -1,
	//	Author: "AAAA AAAA",
	//	Title: "AAAA",
	//	Year: 1500,
	//	Available: true,
	//	Price: 1.99,
	//	Published: t3,
	//}
	//
	//t4, err := time.Parse(time.RFC3339, "1200-01-01T10:00:00Z")
	//panicOnErr(err)
	//b4 := Book{
	//	Id: -1,
	//	Author: "BBBB BBBB",
	//	Title: "BBBB",
	//	Year: 1200,
	//	Available: true,
	//	Price: 1.99,
	//	Published: t4,
	//}
	//
	//t5, err := time.Parse(time.RFC3339, "1200-01-01T09:00:00Z")
	//panicOnErr(err)
	//b5 := Book{
	//	Id: -1,
	//	Author: "CCCC CCCC",
	//	Title: "CCCC",
	//	Year: 1200,
	//	Available: true,
	//	Price: 1.99,
	//	Published: t5,
	//}
}

func OpenTestDB(t *testing.T, options ...PureDBOptionFn) *PureDB {
	db, err := Open(TempFileName("puredb-", ".db"), options...)
	if err != nil {
		t.Fatal("can't open db", err)
	}
	return db
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func TempFileName(prefix, suffix string) string {
	tf, err := ioutil.TempFile("", prefix)
	panicOnErr(err)

	panicOnErr(tf.Close())

	filename := tf.Name()

	panicOnErr(os.Remove(filename))

	return filename + suffix
}
