package puredb

import (
	"reflect"
	"fmt"
	"github.com/dgraph-io/badger"
)

type Table struct {
	DB *PureDB
	badgerDB *badger.DB

	Name string

	buckets *buckets

	structInfo	*structInfo
}

type structInfo struct {
	pTyp	reflect.Type
	typ		reflect.Type

	fields	[]*reflect.StructField
	fieldByName map[string]*reflect.StructField

	indexes	[]*indexInfo
	primary	*indexInfo
	indexByName map[string]*indexInfo
}

type indexInfo struct {
	name		string
	primary		bool
	unique		bool
	field		*reflect.StructField
	valueType	*reflect.Type
	bucketOpts	BucketOpts
	bucket		*Bucket
}

func (table *Table) Badger() *badger.DB {
	return table.badgerDB
}

func (table *Table) Setup(db *PureDB, name string) error {
	table.DB = db
	table.badgerDB = db.DB
	table.Name = name

	table.buckets = &buckets{}
	table.buckets.Init(table.DB)

	return nil
}

func (table *Table) Cleanup() {
	table.buckets.Cleanup()
}

func (table *Table) GetName() string {
	return table.Name
}

func (table *Table) GetOrCreateBucket(name string, opts BucketOpts) *Bucket {
	finalBucketName := table.Name + "." + name
	if table.buckets.Has(finalBucketName) {
		return table.buckets.Get(finalBucketName)
	}
	bucket, err := table.buckets.Add(finalBucketName, opts)
	if err != nil {
		panic(err)
	}
	return bucket
}

func (table *Table) TxnSave(txnManager TransactionManager, v interface{}) (int64, error) {
	sInfo, err := table.getStructInfo(v)
	if err != nil {
		return -1, err
	}

	pVal := reflect.ValueOf(v)
	val := pVal.Elem()

	primaryId := int64(-1)

	err = txnManager.UpdateWithNested(func(nestedTxnMgr *NopNestedTransactionManager) error {
		// integrity check
		for _, index := range sInfo.indexes {
			if ! (index.primary || index.unique) {
				continue
			}
			indexValuePtr := reflect.New(*index.valueType)
			//indexValue := indexValuePtr.Elem()
			fieldVal := val.FieldByIndex(index.field.Index)
			//fmt.Fprintf(os.Stderr, "checking index %v for value:%v indexValue:%v\n", index.name, fieldVal.Interface(), indexValue.Interface())
			err = index.bucket.TxnGet(nestedTxnMgr, fieldVal.Interface(), indexValuePtr.Interface())
			switch err {
			case badger.ErrKeyNotFound:
				continue // ok, field value is not present in the unique index

			case nil:
				// error, an entry is already present for this index value
				return NewDuplicateKeyError(index, fieldVal.Interface())

			default:
				return err
			}
		}

		primaryId, err = sInfo.primary.bucket.TxnAdd(nestedTxnMgr, v)
		if err != nil {
			return err
		}

		for _, index := range sInfo.indexes {
			if index.primary {
				continue
			}
			fieldVal := val.FieldByIndex(index.field.Index)
			err = index.bucket.TxnSet(nestedTxnMgr, fieldVal.Interface(), primaryId)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return primaryId, err
}

func (table *Table) TxnGet(txnManager TransactionManager, id int64, v interface{}) error {
	sInfo, err := table.getStructInfo(v)
	if err != nil {
		return err
	}

	primary := sInfo.primary
	//indexValuePtr := reflect.New(*primary.valueType)

	err = txnManager.ViewWithNested(func(nestedTxnMgr *NopNestedTransactionManager) error {
		//err = primary.bucket.Get(id, indexValuePtr.Interface())
		return primary.bucket.TxnGet(nestedTxnMgr, id, v)
	})

	return err
}

func (table *Table) TxnGetBy(txnManager TransactionManager, column string, k interface{}, v interface{}) error {
	sInfo, err := table.getStructInfo(v)
	if err != nil {
		return err
	}

	index, ok := sInfo.indexByName[column]
	if !ok {
		return NewNoSuchIndexError(column, k)
	}
	if !index.unique {
		return NewIndexNotUnique(index, k)
	}
	id := int64(-1)

	err = txnManager.ViewWithNested(func(nestedTxnMgr *NopNestedTransactionManager) error {
		err := index.bucket.TxnGet(nestedTxnMgr, k, &id)
		if err != nil {
			return err
		}
		return sInfo.primary.bucket.TxnGet(nestedTxnMgr, id, v)
	})
	return err
}

func (table *Table) Save(v interface{}) (int64, error) {
	return table.TxnSave(table.DB, v)
}

func (table *Table) Get(id int64, v interface{}) error {
	return table.TxnGet(table.DB, id, v)
}

func (table *Table) GetBy(column string, k interface{}, v interface{}) error {
	return table.TxnGetBy(table.DB, column, k, v)
}

func (table *Table) getStructInfo(v interface{}) (*structInfo, error) {
	pTyp := reflect.TypeOf(v)
	//pVal := reflect.ValueOf(v)
	if pTyp.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("not a pointer")
	}
	typ := pTyp.Elem()
	//val := pVal.Elem()
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("not a pointer to a struct")
	}

	if table.structInfo != nil {
		if (table.structInfo.pTyp != pTyp) || (table.structInfo.typ != typ) {
			return nil, fmt.Errorf("struct doesn't match the blueprint one")
		}
		return table.structInfo, nil
	}

	sInfo := &structInfo{
		pTyp: pTyp,
		typ: typ,
		fields: make([]*reflect.StructField, 0),
		fieldByName: make(map[string]*reflect.StructField),
		indexes: make([]*indexInfo, 0),
		indexByName: make(map[string]*indexInfo),
	}
	table.structInfo = sInfo

	for i := 0; i < typ.NumField(); i++ {
		structField := typ.Field(i)

		fieldName := structField.Name

		sInfo.fields = append(sInfo.fields, &structField)
		sInfo.fieldByName[fieldName] = &structField

		tag, _ := structField.Tag.Lookup("puredb")
		tagOpts := parseTag(tag)

		if tagOpts.Has("primary") {
			primaryBucketOpts := BucketOpts{}
			primaryBucketOpts.PreAddFn = func (bucket *Bucket, k interface{}, v interface{}) error {
				id := k.(int64)
				// reflect.ValueOf(v).Elem().Type().AssignableTo(typ) <=> v.(*myStruct)
				if ! reflect.ValueOf(v).Elem().Type().AssignableTo(typ) {
					return fmt.Errorf("value doesn't implement pointer to (specified) struct interface")
				}

				//typ := reflect.TypeOf(v).Elem()
				val := reflect.ValueOf(v).Elem()
				idFieldVal := val.FieldByName(fieldName)
				if idFieldVal.CanSet() {
					idFieldVal.SetInt(id)
				} else {
					return fmt.Errorf("id field can't be set")
				}
				return nil
			}
			bucket := table.GetOrCreateBucket(structField.Name, primaryBucketOpts)
			primaryIndex := indexInfo{
				name:		fieldName,
				primary:	true,
				unique:		true,
				field:		&structField,
				valueType:  &typ,
				bucketOpts:	primaryBucketOpts,
				bucket:		bucket,
			}
			sInfo.primary = &primaryIndex
			sInfo.indexes = append(sInfo.indexes, &primaryIndex)
			sInfo.indexByName[fieldName] = &primaryIndex
		} else if tagOpts.Has("index") {
			indexBucketOpts := BucketOpts{}

			int64Type := reflect.TypeOf(int64(-1))
			bucket := table.GetOrCreateBucket(structField.Name, indexBucketOpts)
			secondaryIndex := indexInfo{
				name:		fieldName,
				primary:	false,
				unique:		tagOpts.Has("unique"),
				field:		&structField,
				valueType:  &int64Type,
				bucketOpts:	indexBucketOpts,
				bucket:		bucket,
			}
			sInfo.indexes = append(sInfo.indexes, &secondaryIndex)
			sInfo.indexByName[fieldName] = &secondaryIndex
		}
	}

	return sInfo, nil
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}


type tables struct {
	DB	*PureDB
	Map	map[string]*Table
}

func (tables *tables) Init(db *PureDB) {
	tables.DB = db
	tables.Map = make(map[string]*Table)
}

func (tables *tables) Cleanup() {
	for _, ti := range tables.Map {
		ti.Cleanup()
	}
}

func (tables *tables) Add(name string) (*Table, error) {
	table := Table{}
	err := table.Setup(tables.DB, name)
	if err != nil {
		return nil, err
	}
	tables.Map[name] = &table
	return &table, nil
}

func (tables *tables) Get(name string) *Table {
	return tables.Map[name]
}

func (tables *tables) Has(name string) bool {
	_, ok := tables.Map[name]
	return ok
}
