package puredb

//import "github.com/pkg/errors"
import (
	"fmt"
)

type DuplicateKeyError struct {
	msg		string
	index	*indexInfo
	value	interface{}
}

func (e *DuplicateKeyError) Error() string { return e.msg }

func (e *DuplicateKeyError) indexInfo() *indexInfo { return e.index }

func (e *DuplicateKeyError) Key() string { return e.index.name }

func (e *DuplicateKeyError) Value() interface{} { return e.value }

func NewDuplicateKeyError(index *indexInfo, value interface{}, a ...interface{}) error {
	msg := ""
	if len(a) > 0 {
		format, ok := a[0].(string)
		if ok && len(format) > 0 {
			msg = fmt.Sprintf(format, a[1:]...)
		}
	}
	if len(msg) == 0 {
		msg = fmt.Sprintf("value already present for unique index %q (value %v)", index.name, value)
	}
	return &DuplicateKeyError{
		msg: "integrity error - " + msg,
		index: index,
		value: value,
	}
}


type NoSuchIndexError struct {
	msg		string
	key		string
	value	interface{}
}

func (e *NoSuchIndexError) Error() string { return e.msg }

func (e *NoSuchIndexError) Key() string { return e.key }

func (e *NoSuchIndexError) Value() interface{} { return e.value }

func NewNoSuchIndexError(key string, value interface{}, a ...interface{}) error {
	msg := ""
	if len(a) > 0 {
		format, ok := a[0].(string)
		if ok && len(format) > 0 {
			msg = fmt.Sprintf(format, a[1:]...)
		}
	}
	if len(msg) == 0 {
		msg = fmt.Sprintf("no such index %q (value %v)", key, value)
	}
	return &NoSuchIndexError{
		msg: "index error - " + msg,
		key: key,
		value: value,
	}
}

type IndexNotUnique struct {
	msg		string
	index	*indexInfo
	value	interface{}
}

func (e *IndexNotUnique) Error() string { return e.msg }

func (e *IndexNotUnique) indexInfo() *indexInfo { return e.index }

func (e *IndexNotUnique) Key() string { return e.index.name }

func (e *IndexNotUnique) Value() interface{} { return e.value }

func NewIndexNotUnique(index *indexInfo, value interface{}, a ...interface{}) error {
	msg := ""
	if len(a) > 0 {
		format, ok := a[0].(string)
		if ok && len(format) > 0 {
			msg = fmt.Sprintf(format, a[1:]...)
		}
	}
	if len(msg) == 0 {
		msg = fmt.Sprintf("index %q is not unique (value %v)", index.name, value)
	}
	return &IndexNotUnique{
		msg: "index error - " + msg,
		index: index,
		value: value,
	}
}

type NotComparable struct {
	msg		string
	first	interface{}
	second	interface{}
}

func (e *NotComparable) Error() string { return e.msg }

func (e *NotComparable) First() interface{} { return e.first }
func (e *NotComparable) Second() interface{} { return e.second }

func NewNotComparable(first interface{}, second interface{}, a ...interface{}) error {
	msg := ""
	if len(a) > 0 {
		format, ok := a[0].(string)
		if ok && len(format) > 0 {
			msg = fmt.Sprintf(format, a[1:]...)
		}
	}
	if len(msg) == 0 {
		msg = fmt.Sprintf("values are not comparable (first: %v second: %v)", first, second)
	}
	return &NotComparable{
		msg: "not comparable - " + msg,
		first: first,
		second: second,
	}
}
