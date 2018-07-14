// Transaction management
//
// We obviously want to enable the use of transactions
// for all operations supported by the db, with buckets, tables, their iterators, ...
// But we want to maintain an easy-to-use API, without requiring the user to
// always use transactions. For example, the user should be able to
// perform a Get() operation on a table without explictily using transactions,
// but the system should still use them under the covers, to ensure consistent
// results. On the other hand, it should be possible for the user to explicitly
// use transactions, to get consistency across whatever set of operations.
//
// To this end common operations are provided in two variants, a simple one,
// that handles transactions automatically, and an extended one, which
// allows the user to specify the transaction and how it should be handled
// by passing an additional parameter, a transaction manager.
//
// The transaction manager is whatever object that satisfies the
// TransactionManager interface.
//
// The TransactionManager interface
//
//  - provides View() and Update() methods, similar to the ones in BadgerDB
//  - provides ViewWithNested() and UpdateWithNested()
//
// The latter two are similar to View() and Update() but instead of passing
// a freshly created transaction to the called function, they pass
// a NopNestedTransactionManager object.
//
// This NopNestedTransactionManager is TransactionManager which is created
// specifing an existing Transaction and:
//
//  - has View() and Update() methods which don't create a new transaction, but
//    pass along the existing one
//  - has ViewWithNested() and UpdateWithNested() which pass along the
//    NopNestedTransactionManager instance itself
//
// The DB object is a TransactionManager too, and its ViewWithNested() and
// UpdateWithNested() methods return a new NopNestedTransactionManager
// embedding a newly created Transaction.
//
// This allows to have a single implementation of an operation than can
// both create its own transaction or operate within the bounds of an
// existing transaction of its caller (for a more complex operation).
//
// For example, let's consider the "Get" operation of Table.
// We have a Get() method that can be called without any transaction related
// parameter, and a TxnGet() method which receives a TransactionManager.
// The former will be implemented simply by doing:
//
//	func (table *Table) Get(id int64, v interface{}) error {
//	    return table.TxnGet(table.DB, id, v)
//	}
//
// while for TxnGet(), we'll have:
//
//	func (table *Table) TxnGet(txnManager TransactionManager, id int64, v interface{}) error {
//	    ...
//	    err := txnManager.ViewWithNested(func(nestedTxnMgr *NopNestedTransactionManager) error {
//	        return primary.bucket.TxnGet(nestedTxnMgr, id, v)
//	    })
//	    return err
//	}
//
// which is somewhat equivalent to:
//
//	func (table *Table) TxnGet(txnManager TransactionManager, id int64, v interface{}) error {
//	    ...
//	    err := txnManager.View(func(txn *Transaction) error {
//		    nestedTxnMgr := &NopNestedTransactionManager{Txn: txn}
//	        return primary.bucket.TxnGet(nestedTxnMgr, id, v)
//	    })
//	    return err
//	}
//
// We see that when called with:
//
//	table.TxnGet(table.DB, id, v)
//
// TxnGet() will receive the DB as TransactionManager, which will create a new transaction and
// ViewWithNested() will receive a new NopNestedTransactionManager with this embedded transaction.
//
// Otherwise, if we need to call TxnGet() in the context of a larger operation - and thus larger
// single transaction - we can call it passing an already existing NopNestedTransactionManager.
// In this case no new transaction will be created.
//
package puredb

import "fmt"

// TransactionManager is an interface providing the View and Update
// abstractions.
// An implementer of the interface may provide the usual implementation
// as done by PureDB, or may otherwise decide to provide simple
// stubs when it handles transactions in a more ample context,
// as done by TableIter.
// PureDB implements the TransactionManager interface, as well as
// TableIter.
type TransactionManager interface {
	View(fn func(transaction *Transaction) error) error
	Update(fn func(transaction *Transaction) error) error
	ViewWithNested(fn func(nestedTxnMgr *NopNestedTransactionManager) error) error
	UpdateWithNested(fn func(nestedTxnMgr *NopNestedTransactionManager) error) error
}

// NopNestedTransactionManager is a nested TransactionManager whose View() and
// Update() methods do NOT perform automatic Discard or Commit on the transaction
// since it is being used from a more ample bigger transaction context
// (e.g. a bucket Set() called from inside a table Set() operation, that updates
// more than one bucket).
type NopNestedTransactionManager struct {
	Txn *Transaction
}

// View for NopNestedTransactionManager doesn't perform any Discard (or Commit), since it's
// supposedly operating inside a wider transaction context
func (nopTxn *NopNestedTransactionManager) View(fn func(transaction *Transaction) error) error {
	return fn(nopTxn.Txn)
}

// Update for NopNestedTransactionManager doesn't perform any Discard/Commit, since it's
// supposedly operating inside a wider transaction context
func (nopTxn *NopNestedTransactionManager) Update(fn func(transaction *Transaction) error) error {
	if !nopTxn.Txn.RW {
		// XXX TODO: add a proper NotRWTransactionError error to errors
		return fmt.Errorf("not a R/W transaction")
	}
	return fn(nopTxn.Txn)
}

// ViewWithNested simply passes on itself, without creating any new transaction.
func (nopTxn *NopNestedTransactionManager) ViewWithNested(fn func(nestedTxnMgr *NopNestedTransactionManager) error) error {
	return fn(nopTxn)
}

// UpdateWithNested simply passes on itself, without creating any new transaction.
func (nopTxn *NopNestedTransactionManager) UpdateWithNested(fn func(nestedTxnMgr *NopNestedTransactionManager) error) error {
	if !nopTxn.Txn.RW {
		// XXX TODO: add a proper NotRWTransactionError error to errors
		return fmt.Errorf("not a R/W transaction")
	}
	return fn(nopTxn)
}

func GetNopNestedTransactionManager(txn *Transaction) *NopNestedTransactionManager {
	return &NopNestedTransactionManager{Txn: txn}
}
