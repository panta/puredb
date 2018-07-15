package puredb

import (
	"time"
	"testing"
	"log"
	"fmt"
	"os"
)

//const (
//	bucket_id_book = "id_book"
//	bucket_published_id = "published_id"
//)
//
//type Book struct {
//	Id			int64		`puredb:"primary,auto"`
//	Author		string
//	Title		string
//	Year		int			`puredb:"index"`
//	Available	bool
//	Price		float64
//	Published	time.Time	`puredb:"index,unique"`
//}
//
//func (book *Book) Marshal() ([]byte, error) {
//	b := book
//	b.Published = b.Published.UTC()
//	return msgpack.Marshal(b)
//}
//
//func (book *Book) Unmarshal(data []byte) (error) {
//	err := msgpack.Unmarshal(data, book)
//	if err != nil {
//		return err
//	}
//	book.Published = book.Published.UTC()
//	return nil
//}

var bookFixture = []Book{
	Book{
		Id: -1,
		Author: "William Shakespeare",
		Title: "Much Ado About Nothing",
		Year: 1623,
		Available: true,
		Price: 12.34,
		Published: parse_time("1623-01-01T10:00:00Z"),
	},
	Book{
		Id: -1,
		Author: "Daniel Defoe",
		Title: "Robinson Crusoe",
		Year: 1719,
		Available: true,
		Price: 17.99,
		Published: parse_time("1719-01-01T10:00:00Z"),
	},
	Book{
		Id: -1,
		Author: "AAAA AAAA",
		Title: "AAAA",
		Year: 1500,
		Available: true,
		Price: 1.99,
		Published: parse_time("1500-01-01T10:00:00Z"),
	},
	Book{
		Id: -1,
		Author: "BBBB BBBB",
		Title: "BBBB",
		Year: 1200,
		Available: true,
		Price: 1.99,
		Published: parse_time("1200-01-01T10:00:00Z"),
	},
	Book{
		Id: -1,
		Author: "CCCC CCCC",
		Title: "CCCC",
		Year: 1200,
		Available: true,
		Price: 1.99,
		Published: parse_time("1200-01-01T09:00:00Z"),
	},
}

// expects time in time.RFC3339 format (eg. "1623-01-01T10:00:00Z")
func parse_time(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339, timeStr)
	panicOnErr(err)
	return t
}

func TestTableIter(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Destroy()

	bookTable, err := db.AddTable("books")
	if err != nil {
		t.Fatalf("adding table - err:%v", err)
	}

	t_start := time.Now()

	for _, book := range bookFixture {
		_, err = bookTable.Save(&book)
		if err != nil {
			t.Fatalf("adding record - err:%v", err)
		}
	}

	it, err := NewTableIter(bookTable, "Published")
	if err != nil {
		t.Fatalf("can't create iterator: %v", err)
	}
	for ; it.Valid(); it.Next() {
		var published time.Time
		var book Book
		err = it.Get(&published, &book)
		if err != nil {
			t.Fatalf("error during iteration: %v", err)
		}
		fmt.Fprintf(os.Stderr, "Published:%v Book:%v\n", published, book)
	}
	it.Close()

	log.Printf("Elapsed: %v", time.Now().Sub(t_start))
}
