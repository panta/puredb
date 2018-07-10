package puredb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"reflect"
)

type Serializable interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) (error)
}

func Marshal(v interface{}) ([]byte, error) {
	switch c := v.(type) {
	case Serializable:
		return c.Marshal()

	case *Serializable:
		return (*c).Marshal()

	case []byte:
		return c, nil

	case *[]byte:
		return *c, nil

	case byte:
		return []byte{c}, nil

	case *byte:
		return []byte{*c}, nil

	case string:
		return []byte(c), nil

	case *string:
		return []byte(*c), nil

		// as an optimization, we handle int64 and *int64 explicitly here
		// (since we use those for table ids)
	case int64:
		return i64tob(c), nil

	case *int64:
		return i64tob(*c), nil

	case float32, *float32, []float32,
	float64, *float64, []float64,
	bool, *bool, []bool,
	int8, *int8, []int8,
	int16, *int16, []int16,
	uint16, *uint16, []uint16,
	int32, *int32, []int32,
	uint32, *uint32, []uint32,
	[]int64,
	uint64, *uint64, []uint64:
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.BigEndian, c)
		if err != nil {
			return []byte{}, err
		}
		return buf.Bytes(), nil

	case time.Time:
		return c.MarshalBinary()

	case *time.Time:
		return (*c).MarshalBinary()

	default:
		return []byte{}, fmt.Errorf("value not supported by Marshal v:%v type:%v (%v)", v, reflect.TypeOf(v), reflect.TypeOf(v).String())
	}

	return []byte{}, fmt.Errorf("value not supported by Marshal v:%v type:%v (%v)", v, reflect.TypeOf(v), reflect.TypeOf(v).String())
}

func Unmarshal(data []byte, v interface{}) (error) {
	switch c := v.(type) {
	case Serializable:
		return c.Unmarshal(data)

	case *Serializable:
		return (*c).Unmarshal(data)

	case []byte:
		c = data
		return nil

	case *[]byte:
		*c = data
		return nil

	case byte:
		c = data[0]
		return nil

	case *byte:
		*c = data[0]
		return nil

	case string:
		c = string(data)

	case *string:
		*c = string(data)

		// as an optimization, we handle int64 and *int64 explicitly here
		// (since we use those for table ids)
	case int64:
		c = int64(binary.BigEndian.Uint64(data))
		return nil

	case *int64:
		*c = int64(binary.BigEndian.Uint64(data))
		return nil

	case float32, *float32, []float32,
	float64, *float64, []float64,
	bool, *bool, []bool,
	int8, *int8, []int8,
	int16, *int16, []int16,
	uint16, *uint16, []uint16,
	int32, *int32, []int32,
	uint32, *uint32, []uint32,
	[]int64,
	uint64, *uint64, []uint64:
		buf := bytes.NewBuffer(data)
		err := binary.Read(buf, binary.BigEndian, c)
		if err != nil {
			return err
		}
		return nil

	case time.Time:
		return c.UnmarshalBinary(data)

	case *time.Time:
		return (*c).UnmarshalBinary(data)

	default:
		return fmt.Errorf("value not supported by Unmarshal v:%v type:%v (%v)", v, reflect.TypeOf(v), reflect.TypeOf(v).String())
	}

	return fmt.Errorf("value not supported by Unmarshal v:%v type:%v (%v)", v, reflect.TypeOf(v), reflect.TypeOf(v).String())
}

