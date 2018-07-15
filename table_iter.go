package puredb

import (
	"reflect"
	"bytes"
)

type TableIterKeyCallback func(tableIter *TableIter, k []byte) (bool, error)
type TableIterValueCallback func(tableIter *TableIter, id int64, v interface{}) (bool, error)

type TableIterOptionFn func (tableIter *TableIter) error

type TableIterOpts struct {
	Column		string
	Begin		interface{}
	End			interface{}
	Reverse		bool
}

type TableIter struct {
	Table		*Table
	Column		string
	Err			error

	KeyFn		TableIterKeyCallback			// callback called during key iterations
	ValueFn		TableIterValueCallback			// callback called during iteration upon record resolution/retrieval

	Reverse 	bool
	prefix		[]byte

	Txn      *Transaction
	txnMgr	 *NopNestedTransactionManager
	index    *indexInfo
	bucketIt *BucketIter
}


func NewTableIter(table *Table, column string, options ...TableIterOptionFn) (*TableIter, error) {
	txn := table.DB.NewReadOnlyTransaction()
	it := TableIter{
		Table:   table,
		Column:  column,
		Txn:     txn,
		txnMgr:	 &NopNestedTransactionManager{Txn: txn},
		KeyFn:   tableIterKeyAllCb,
		ValueFn: tableIterValueAllCb,
	}

	index, ok := table.structInfo.indexByName[it.Column]
	it.index = index
	if !ok {
		it.Err = NewNoSuchIndexError(it.Column, nil)
		return &it, it.Err
	}

	for _, option := range options {
		err := option(&it)
		if err != nil {
			return nil, err
		}
	}

	it.bucketIt = NewBucketIter(index.bucket, BucketIterOpts{
		Prefix: it.prefix,
		Reverse: it.Reverse,
		ExternalTxn: it.Txn,
	})

	it.Rewind()

	return &it, nil
}

func (it *TableIter) Close() {
	it.bucketIt.Close()
	it.Txn.Close()
}

func (it *TableIter) Rewind() {
	it.bucketIt.Rewind()
}

func (it *TableIter) Valid() bool {
	return it.bucketIt.Valid()
}

func (it *TableIter) EOF() bool {
	return (! it.bucketIt.EOF())
}

func (it *TableIter) Next() {
	it.bucketIt.Next()
}

func (it *TableIter) Error() bool {
	return (it.Err != nil) || (it.bucketIt.Error())
}

func (it *TableIter) Get(keyp interface{}, valuep interface{}) error {
	if it.index.primary {
		return it.bucketIt.Get(keyp, valuep)
	} else {
		//keypVal := reflect.New(it.index.field.Type)
		id := int64(-1)
		err := it.bucketIt.Get(keyp, &id)
		//err := it.bucketIt.Get(keypVal.Interface(), &id)
		if err != nil {
			return err
		}

		return it.Table.structInfo.primary.bucket.TxnGet(it.txnMgr, id, valuep)
	}
}

func (it *TableIter) Iterate(keyPtr interface{}, valuePtr interface{}, cmpFn TableIterValueCallback) (bool, error) {
	doContinue := true

	for it.Rewind(); it.Valid() && doContinue; it.Next() {
		keyBinary := make([]byte, 0)
		err := it.bucketIt.GetBinaryKey(keyBinary)
		if err != nil {
			return false, err
		}

		if it.KeyFn != nil {
			cont, err := it.KeyFn(it, keyBinary)
			if err != nil {
				return false, err
			}
			doContinue = cont
		}

		if !doContinue {
			break
		}

		if it.ValueFn != nil {
			id := int64(-1)

			if it.index.primary {
				err = Unmarshal(keyBinary, &id)
				if err != nil {
					return false, err
				}

				keyPtr = id

				err = it.bucketIt.Get(id, valuePtr)
				if err != nil {
					return false, err
				}
			} else {
				// get current key for selected index and id
				keyp := reflect.New(it.index.field.Type)
				err := it.bucketIt.Get(keyp, &id)
				if err != nil {
					return false, err
				}

				// get record
				err = it.Table.structInfo.primary.bucket.TxnGet(it.txnMgr, id, valuePtr)
				if err != nil {
					return false, err
				}
			}

			cont, err := it.ValueFn(it, id, valuePtr)
			if err != nil {
				return false, err
			}
			doContinue = cont
		}
	}

	return false, nil
}

func (it *TableIter) Find(idPtr *int64, valuePtr interface{}, foundFn TableIterValueCallback) (bool, error) {
	doContinue := true

	for it.Rewind(); it.Valid() && doContinue; it.Next() {
		keyBinary := make([]byte, 0)
		err := it.bucketIt.GetBinaryKey(keyBinary)
		if err != nil {
			return false, err
		}

		if it.KeyFn != nil {
			cont, err := it.KeyFn(it, keyBinary)
			if err != nil {
				return false, err
			}
			doContinue = cont
		}

		if !doContinue {
			break
		}

		if (it.ValueFn != nil) || (foundFn != nil) {
			id := int64(-1)

			if it.index.primary {
				err = Unmarshal(keyBinary, &id)
				if err != nil {
					return false, err
				}

				*idPtr = id

				err = it.bucketIt.Get(id, valuePtr)
				if err != nil {
					return false, err
				}
			} else {
				// get current key for selected index and id
				keyp := reflect.New(it.index.field.Type)
				err := it.bucketIt.Get(keyp, &id)
				if err != nil {
					return false, err
				}

				*idPtr = id

				// get record
				err = it.Table.structInfo.primary.bucket.TxnGet(it.txnMgr, id, valuePtr)
				if err != nil {
					return false, err
				}
			}

			if it.ValueFn != nil {
				cont, err := it.ValueFn(it, id, valuePtr)
				if err != nil {
					return false, err
				}
				doContinue = cont
			}

			if !doContinue {
				break
			}

			if foundFn != nil {
				found, err := foundFn(it, id, valuePtr)
				if err != nil {
					return false, err
				}
				if found {
					return true, nil
				}
			}
		}
	}

	return false, nil
}


// --------------------------------------------------------------------------
// option functions
// --------------------------------------------------------------------------

// tableIterAllCb iterates over all iterator range
func tableIterKeyAllCb(tableIter *TableIter, k []byte) (bool, error) {
	return true, nil
}

func tableIterValueAllCb(tableIter *TableIter, id int64, v interface{}) (bool, error) {
	return true, nil
}

// TableIterOptionGT sets up for iteration over keys y > x
func TableIterOptionGT(tableIter *TableIter, x interface{}) error {
	xBinary, err := Marshal(x)
	if err != nil {
		return err
	}

	if !tableIter.Reverse {
		tableIter.prefix = xBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return bytes.Compare(xBinary, y) < 0, nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionGE sets up for iteration over keys y >= x
func TableIterOptionGE(tableIter *TableIter, x interface{}) error {
	xBinary, err := Marshal(x)
	if err != nil {
		return err
	}

	if !tableIter.Reverse {
		tableIter.prefix = xBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return bytes.Compare(xBinary, y) <= 0, nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionEQ sets up for iteration over keys y == x
func TableIterOptionEQ(tableIter *TableIter, x interface{}) error {
	xBinary, err := Marshal(x)
	if err != nil {
		return err
	}

	tableIter.prefix = xBinary

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return bytes.Compare(xBinary, y) == 0, nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionLT sets up for iteration over keys y < x
func TableIterOptionLT(tableIter *TableIter, x interface{}) error {
	xBinary, err := Marshal(x)
	if err != nil {
		return err
	}

	if tableIter.Reverse {
		tableIter.prefix = xBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return bytes.Compare(xBinary, y) > 0, nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionLE sets up for iteration over keys y <= x
func TableIterOptionLE(tableIter *TableIter, x interface{}) error {
	xBinary, err := Marshal(x)
	if err != nil {
		return err
	}

	if !tableIter.Reverse {
		tableIter.prefix = xBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return bytes.Compare(xBinary, y) >= 0, nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionRangeInclusive sets up for iteration over keys  lo <= y <= hi
func TableIterOptionRangeInclusive(tableIter *TableIter, lo interface{}, hi interface{}) error {
	loBinary, err := Marshal(lo)
	if err != nil {
		return err
	}
	hiBinary, err := Marshal(hi)
	if err != nil {
		return err
	}

	if !tableIter.Reverse {
		tableIter.prefix = loBinary
	} else {
		tableIter.prefix = hiBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return (bytes.Compare(loBinary, y) <= 0) && (bytes.Compare(hiBinary, y) >= 0), nil
	}
	tableIter.KeyFn = newFn
	return nil
}

// TableIterOptionRangeInclusive sets up for iteration over keys  lo < y < hi
func TableIterOptionRangeExclusive(tableIter *TableIter, lo interface{}, hi interface{}) error {
	loBinary, err := Marshal(lo)
	if err != nil {
		return err
	}
	hiBinary, err := Marshal(hi)
	if err != nil {
		return err
	}

	if !tableIter.Reverse {
		tableIter.prefix = loBinary
	} else {
		tableIter.prefix = hiBinary
	}

	prevFn := tableIter.KeyFn
	if prevFn == nil {
		prevFn = tableIterKeyAllCb
	}
	newFn := func (tableIter *TableIter, y []byte) (bool, error) {
		return (bytes.Compare(loBinary, y) < 0) && (bytes.Compare(hiBinary, y) > 0), nil
	}
	tableIter.KeyFn = newFn
	return nil
}
