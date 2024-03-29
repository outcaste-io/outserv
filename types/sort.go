// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package types

import (
	"math/big"
	"sort"
	"time"

	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"golang.org/x/text/collate"
)

type sortBase struct {
	values [][]Val // Each uid could have multiple values which we need to sort it by.
	desc   []bool  // Sort orders for different values.
	ul     *[]uint64
	cl     *collate.Collator // Compares Unicode strings according to the given collation order.
}

// Len returns size of vector.
// skipcq: CRT-P0003
func (s sortBase) Len() int { return len(s.values) }

// Swap swaps two elements.
// skipcq: CRT-P0003
func (s sortBase) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
	(*s.ul)[i], (*s.ul)[j] = (*s.ul)[j], (*s.ul)[i]
}

type byValue struct{ sortBase }

// Less compares two elements
// skipcq: CRT-P0003
func (s byValue) Less(i, j int) bool {
	first, second := s.values[i], s.values[j]
	if len(first) == 0 || len(second) == 0 {
		return false
	}
	for vidx := range first {
		// Null values are appended at the end of the sort result for both ascending and descending.
		// If both first and second has nil values, then maintain the order by UID.
		if first[vidx].Value == nil && second[vidx].Value == nil {
			return s.desc[vidx]
		}

		if first[vidx].Value == nil {
			return false
		}

		if second[vidx].Value == nil {
			return true
		}

		// We have to look at next value to decide.
		if eq := equal(first[vidx], second[vidx]); eq {
			continue
		}

		// Its either less or greater.
		less := less(first[vidx], second[vidx], s.cl)
		if s.desc[vidx] {
			return !less
		}
		return less
	}
	return false
}

// IsSortable returns true, if tid is sortable. Otherwise it returns false.
func IsSortable(tid TypeID) bool {
	switch tid {
	case TypeDatetime, TypeInt64, TypeBigInt, TypeFloat, TypeString, TypeDefault:
		return true
	default:
		return false
	}
}

// SortWithFacet sorts the given array in-place and considers the given facets to calculate
// the proper ordering.
func SortWithFacet(v [][]Val, ul *[]uint64, desc []bool) error {
	if len(v) == 0 || len(v[0]) == 0 {
		return nil
	}

	for _, val := range v[0] {
		if !IsSortable(val.Tid) {
			return errors.Errorf("Value of type: %s isn't sortable", val.Tid)
		}
	}

	var cl *collate.Collator
	b := sortBase{v, desc, ul, cl}
	toBeSorted := byValue{b}
	sort.Sort(toBeSorted)
	return nil
}

// Sort sorts the given array in-place.
func Sort(v [][]Val, ul *[]uint64, desc []bool) error {
	return SortWithFacet(v, ul, desc)
}

// Less returns true if a is strictly less than b.
func Less(a, b Val) (bool, error) {
	if a.Tid != b.Tid {
		return false, errors.Errorf("Arguments of different type can not be compared.")
	}
	typ := a.Tid
	switch typ {
	case TypeDatetime, TypeUid, TypeInt64, TypeBigInt, TypeFloat, TypeString, TypeDefault:
		// Don't do anything, we can sort values of this type.
	default:
		return false, errors.Errorf("Compare not supported for type: %v", a.Tid)
	}
	return less(a, b, nil), nil
}

func less(a, b Val, cl *collate.Collator) bool {
	if a.Tid != b.Tid {
		return mismatchedLess(a, b)
	}
	switch a.Tid {
	case TypeDatetime:
		return a.Value.(time.Time).Before(b.Value.(time.Time))
	case TypeInt64:
		return (a.Value.(int64)) < (b.Value.(int64))
	case TypeFloat:
		return (a.Value.(float64)) < (b.Value.(float64))
	case TypeBigInt:
		av := a.Value.(big.Int)
		bv := b.Value.(big.Int)
		return av.Cmp(&bv) < 0
	case TypeUid:
		return (a.Value.(uint64) < b.Value.(uint64))
	case TypeString, TypeDefault:
		// Use language comparator.
		if cl != nil {
			return cl.CompareString(a.Safe().(string), b.Safe().(string)) < 0
		}
		return (a.Safe().(string)) < (b.Safe().(string))
	}
	return false
}

func mismatchedLess(a, b Val) bool {
	x.AssertTrue(a.Tid != b.Tid)

	// Floats, ints, bigints can be sorted together in a sensible way. The approach
	// here isn't 100% correct, and will be wrong when dealing with ints and
	// floats close to each other and greater in magnitude than 1<<53 (the
	// point at which consecutive floats are more than 1 apart).
	switch a.Tid {
	case TypeFloat:
		av := a.Value.(float64)
		switch b.Tid {
		case TypeInt64:
			return av < float64(b.Value.(int64))
		case TypeBigInt:
			af := &big.Float{}
			bf := big.NewFloat(av)
			bv := b.Value.(big.Int)
			bf = bf.SetInt(&bv)
			return af.Cmp(bf) < 0
		}
	case TypeInt64:
		av := a.Value.(int64)
		switch b.Tid {
		case TypeFloat:
			return float64(av) < b.Value.(float64)
		case TypeBigInt:
			ai := big.NewInt(av)
			bi := b.Value.(big.Int)
			return ai.Cmp(&bi) < 0
		}
	case TypeBigInt:
		av := a.Value.(big.Int)
		switch b.Tid {
		case TypeFloat:
			af := &big.Float{}
			af = af.SetInt(&av)
			aff, _ := af.Float64()
			return aff < b.Value.(float64)
		case TypeBigInt:
			ai := a.Value.(big.Int)
			bi := b.Value.(big.Int)
			return ai.Cmp(&bi) < 0
		}
	}

	// Non-float/int are sorted arbitrarily by type.
	return a.Tid < b.Tid
}

// Equal returns true if a is equal to b.
func Equal(a, b Val) (bool, error) {
	if a.Tid != b.Tid {
		return false, errors.Errorf("Arguments of different type can not be compared.")
	}
	typ := a.Tid
	switch typ {
	case TypeDatetime, TypeInt64, TypeFloat, TypeString, TypeDefault, TypeBool:
		// Don't do anything, we can sort values of this type.
	default:
		return false, errors.Errorf("Equal not supported for type: %v", a.Tid)
	}
	return equal(a, b), nil
}

func equal(a, b Val) bool {
	if a.Tid != b.Tid {
		return false
	}
	switch a.Tid {
	case TypeDatetime:
		aVal, aOk := a.Value.(time.Time)
		bVal, bOk := b.Value.(time.Time)
		return aOk && bOk && aVal.Equal(bVal)
	case TypeInt64:
		aVal, aOk := a.Value.(int64)
		bVal, bOk := b.Value.(int64)
		return aOk && bOk && aVal == bVal
	case TypeBigInt:
		av, aOk := a.Value.(big.Int)
		bv, bOk := b.Value.(big.Int)
		return aOk && bOk && av.Cmp(&bv) == 0
	case TypeFloat:
		aVal, aOk := a.Value.(float64)
		bVal, bOk := b.Value.(float64)
		return aOk && bOk && aVal == bVal
	case TypeString, TypeDefault:
		aVal, aOk := a.Value.(string)
		bVal, bOk := b.Value.(string)
		return aOk && bOk && aVal == bVal
	case TypeBool:
		aVal, aOk := a.Value.(bool)
		bVal, bOk := b.Value.(bool)
		return aOk && bOk && aVal == bVal
	}
	return false
}
