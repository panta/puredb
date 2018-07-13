package puredb

import "reflect"

type Comparable interface {
	Cmp(comparable Comparable) int
	Less(comparable Comparable) bool
}

func Cmp(x, y interface{}) (int, error) {
	switch cx := x.(type) {
	case Comparable:
		switch cy := y.(type) {
		case Comparable:
			return cx.Cmp(cy), nil
		case *Comparable:
			return cx.Cmp(*cy), nil
		}

	case *Comparable:
		switch cy := y.(type) {
		case Comparable:
			return (*cx).Cmp(cy), nil
		case *Comparable:
			return (*cx).Cmp(*cy), nil
		}
	}

	return -1, NewNotComparable(x, y)
}

func Less(x, y interface{}) (bool, error) {
	cmp, err := Cmp(x, y)
	if err != nil {
		return true, err
	}

	switch {
	case cmp < 0:
		return true, nil
	default:
		return false, nil
	}
}

func LessOrEqual(x, y interface{}) (bool, error) {
	cmp, err := Cmp(x, y)
	if err != nil {
		return true, err
	}

	switch {
	case cmp <= 0:
		return true, nil
	default:
		return false, nil
	}
}

func Greater(x, y interface{}) (bool, error) {
	cmp, err := Cmp(x, y)
	if err != nil {
		return true, err
	}

	switch {
	case cmp > 0:
		return true, nil
	default:
		return false, nil
	}
}

func GreaterOrEqual(x, y interface{}) (bool, error) {
	cmp, err := Cmp(x, y)
	if err != nil {
		return true, err
	}

	switch {
	case cmp >= 0:
		return true, nil
	default:
		return false, nil
	}
}

func Equal(x, y interface{}) bool {
	if x == nil || y == nil {
		return x == y
	}

	switch cx := x.(type) {
	case Comparable:
		switch cy := y.(type) {
		case Comparable:
			return cx.Cmp(cy) == 0
		case *Comparable:
			return cx.Cmp(*cy) == 0
		}

	case *Comparable:
		switch cy := y.(type) {
		case Comparable:
			return (*cx).Cmp(cy) == 0
		case *Comparable:
			return (*cx).Cmp(*cy) == 0
		}
	}

	return reflect.DeepEqual(x, y)
}

func Different(x, y interface{}) bool {
	if x == nil || y == nil {
		return x != y
	}

	return !Equal(x, y)
}
