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
