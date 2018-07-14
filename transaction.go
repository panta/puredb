package puredb

import (
	"github.com/dgraph-io/badger"
)

// Transaction represents a transaction in the underlying storage
// engine.
type Transaction struct {
	badgerTxn *badger.Txn
	RW        bool
	Err       error
	Committed bool
	Discarded bool
}

func NewReadOnlyTransaction(db *PureDB) *Transaction {
	badgerDB := db.DB
	return &Transaction{
		badgerTxn: badgerDB.NewTransaction(false), // read-only transaction (update set to false)
		RW:        false,
		Err:       nil,
		Committed: false,
		Discarded: false,
	}
}

func NewReadWriteTransaction(db *PureDB) *Transaction {
	badgerDB := db.DB
	return &Transaction{
		badgerTxn: badgerDB.NewTransaction(true), // R/W transaction (update set to true)
		RW:        true,
		Err:       nil,
		Committed: false,
		Discarded: false,
	}
}

func (transaction *Transaction) Commit() error {
	if transaction.RW {
		err := transaction.badgerTxn.Commit(nil)
		transaction.Discarded = true		// badgerTxn.Commit() internally calls Discard()
		if err != nil {
			transaction.Err = err
		} else {
			transaction.Committed = true
		}
		return err
	} else {
		transaction.badgerTxn.Discard()
		transaction.Discarded = true
		return nil
	}
}

func (transaction *Transaction) Discard() {
	if !transaction.Discarded {
		transaction.badgerTxn.Discard()
		transaction.Discarded = true
	}
}

func (transaction *Transaction) Close() {
	if !transaction.RW {
		transaction.Discard()
	} else if transaction.RW && (!transaction.Committed) && (transaction.Err == nil) {
		transaction.Commit()
	}

	transaction.Discard()
}

func (transaction *Transaction) Error() error {
	return transaction.Err
}

func (transaction *Transaction) IsOpen() bool {
	return (!transaction.Committed) && (!transaction.Discarded) && (transaction.Err == nil)
}

func (transaction *Transaction) IsClosed() bool {
	return !transaction.IsOpen()
}
