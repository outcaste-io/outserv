// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package query

import (
	"math"
	"math/big"
	"time"

	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

type aggregator struct {
	name   string
	result types.Val
	count  int // used when we need avergae.
}

func isUnary(f string) bool {
	return f == "ln" || f == "exp" || f == "u-" || f == "sqrt" ||
		f == "floor" || f == "ceil" || f == "since"
}

func isBinaryBoolean(f string) bool {
	return f == "<" || f == ">" || f == "<=" || f == ">=" ||
		f == "==" || f == "!="
}

func isTernary(f string) bool {
	return f == "cond"
}

func isBinary(f string) bool {
	return f == "+" || f == "*" || f == "-" || f == "/" || f == "%" ||
		f == "max" || f == "min" || f == "logbase" || f == "pow"
}

func compareValues(ag string, va, vb types.Val) (bool, error) {
	if !isBinaryBoolean(ag) {
		x.Fatalf("Function %v is not binary boolean", ag)
	}

	if _, err := types.Less(va, vb); err != nil {
		// Try to convert values.
		switch {
		case va.Tid == types.TypeInt64:
			va.Tid = types.TypeFloat
			va.Value = float64(va.Value.(int64))
		case vb.Tid == types.TypeInt64:
			vb.Tid = types.TypeFloat
			vb.Value = float64(vb.Value.(int64))
		case vb.Tid == types.TypeBigInt:
			vb.Tid = types.TypeFloat
			vi := vb.Value.(big.Int)
			vb.Value = float64(vi.Int64())
		default:
			return false, err
		}
	}
	isLess, err := types.Less(va, vb)
	if err != nil {
		return false, err
	}
	isMore, err := types.Less(vb, va)
	if err != nil {
		return false, err
	}
	isEqual, err := types.Equal(va, vb)
	if err != nil {
		return false, err
	}
	switch ag {
	case "<":
		return isLess, nil
	case ">":
		return isMore, nil
	case "<=":
		return isLess || isEqual, nil
	case ">=":
		return isMore || isEqual, nil
	case "==":
		return isEqual, nil
	case "!=":
		return !isEqual, nil
	}
	return false, errors.Errorf("Invalid compare function %q", ag)
}

func applyAdd(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		aVal, bVal := a.Value.(int64), b.Value.(int64)

		if (aVal > 0 && bVal > math.MaxInt64-aVal) ||
			(aVal < 0 && bVal < math.MinInt64-aVal) {
			return ErrorIntOverflow
		}

		c.Value = aVal + bVal

	case FLOAT:
		c.Value = a.Value.(float64) + b.Value.(float64)

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		c.Value = va.Add(&va, &vb)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func +", a.Tid)
	}
	return nil
}

func applySub(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		aVal, bVal := a.Value.(int64), b.Value.(int64)

		if (bVal < 0 && aVal > math.MaxInt64+bVal) ||
			(bVal > 0 && aVal < math.MinInt64+bVal) {
			return ErrorIntOverflow
		}

		c.Value = aVal - bVal

	case FLOAT:
		c.Value = a.Value.(float64) - b.Value.(float64)

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		c.Value = va.Sub(&va, &vb)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func -", a.Tid)
	}
	return nil
}

func applyMul(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		aVal, bVal := a.Value.(int64), b.Value.(int64)
		c.Value = aVal * bVal

		if aVal == 0 || bVal == 0 {
			return nil
		} else if c.Value.(int64)/bVal != aVal {
			return ErrorIntOverflow
		}

	case FLOAT:
		c.Value = a.Value.(float64) * b.Value.(float64)

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		c.Value = va.Mul(&va, &vb)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func *", a.Tid)
	}
	return nil
}

func applyDiv(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		if b.Value.(int64) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = a.Value.(int64) / b.Value.(int64)

	case FLOAT:
		if b.Value.(float64) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = a.Value.(float64) / b.Value.(float64)

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		if vb.Cmp(big.NewInt(0)) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = va.Div(&va, &vb)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func /", a.Tid)
	}
	return nil
}

func applyMod(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		if b.Value.(int64) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = a.Value.(int64) % b.Value.(int64)

	case FLOAT:
		if b.Value.(float64) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = math.Mod(a.Value.(float64), b.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		if vb.Cmp(big.NewInt(0)) == 0 {
			return ErrorDivisionByZero
		}
		c.Value = va.Mod(&va, &vb)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func %%", a.Tid)
	}
	return nil
}

func applyPow(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		c.Value = math.Pow(float64(a.Value.(int64)), float64(b.Value.(int64)))
		c.Tid = types.TypeFloat

	case FLOAT:
		// Fractional power of -ve numbers should not be returned.
		if a.Value.(float64) < 0 &&
			math.Abs(math.Ceil(b.Value.(float64))-b.Value.(float64)) > 0 {
			return ErrorFractionalPower
		}
		c.Value = math.Pow(a.Value.(float64), b.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		c.Value = va.Exp(&va, &vb, nil)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func ^", a.Tid)
	}
	return nil
}

func applyLog(a, b, c *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		if a.Value.(int64) < 0 || b.Value.(int64) < 0 {
			return ErrorNegativeLog
		} else if b.Value.(int64) == 1 {
			return ErrorDivisionByZero
		}
		c.Value = math.Log(float64(a.Value.(int64))) / math.Log(float64(b.Value.(int64)))
		c.Tid = types.TypeFloat

	case FLOAT:
		if a.Value.(float64) < 0 || b.Value.(float64) < 0 {
			return ErrorNegativeLog
		} else if b.Value.(float64) == 1 {
			return ErrorDivisionByZero
		}
		c.Value = math.Log(a.Value.(float64)) / math.Log(b.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		vb := a.Value.(big.Int)
		a.Tid = types.TypeInt64
		a.Value = va.Int64()
		b.Tid = types.TypeInt64
		b.Value = vb.Int64()
		return applyLog(a, b, c)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func log", a.Tid)
	}
	return nil
}

func applyMin(a, b, c *types.Val) error {
	r, err := types.Less(*a, *b)
	if err != nil {
		return err
	}
	if r {
		*c = *a
		return nil
	}
	*c = *b
	return nil
}

func applyMax(a, b, c *types.Val) error {
	r, err := types.Less(*a, *b)
	if err != nil {
		return err
	}
	if r {
		*c = *b
		return nil
	}
	*c = *a
	return nil
}

func applyLn(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		if a.Value.(int64) < 0 {
			return ErrorNegativeLog
		}
		res.Value = math.Log(float64(a.Value.(int64)))
		res.Tid = types.TypeFloat

	case FLOAT:
		if a.Value.(float64) < 0 {
			return ErrorNegativeLog
		}
		res.Value = math.Log(a.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		a.Tid = types.TypeInt64
		a.Value = va.Int64()
		return applyLn(a, res)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func ln", a.Tid)
	}
	return nil
}

func applyExp(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		res.Value = math.Exp(float64(a.Value.(int64)))
		res.Tid = types.TypeFloat

	case FLOAT:
		res.Value = math.Exp(a.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		a.Tid = types.TypeInt64
		a.Value = va.Int64()
		return applyExp(a, res)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func exp", a.Tid)
	}
	return nil
}

func applyNeg(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		// -ve of math.MinInt64 is evaluated as itself (due to overflow)
		if a.Value.(int64) == math.MinInt64 {
			return ErrorIntOverflow
		}
		res.Value = -a.Value.(int64)

	case FLOAT:
		res.Value = -a.Value.(float64)

	case BIGINT:
		va := a.Value.(big.Int)
		res.Value = va.Neg(&va)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func u-", a.Tid)
	}
	return nil
}

func applySqrt(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		if a.Value.(int64) < 0 {
			return ErrorNegativeRoot
		}
		res.Value = math.Sqrt(float64(a.Value.(int64)))
		res.Tid = types.TypeFloat

	case FLOAT:
		if a.Value.(float64) < 0 {
			return ErrorNegativeRoot
		}
		res.Value = math.Sqrt(a.Value.(float64))

	case BIGINT:
		va := a.Value.(big.Int)
		res.Value = va.Sqrt(&va)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func sqrt", a.Tid)
	}
	return nil
}

func applyFloor(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		res.Value = a.Value.(int64)

	case FLOAT:
		res.Value = math.Floor(a.Value.(float64))

	case BIGINT:
		res.Value = a.Value.(big.Int)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for func floor", a.Tid)
	}
	return nil
}

func applyCeil(a, res *types.Val) error {
	vBase := getValType(a)
	switch vBase {
	case INT:
		res.Value = a.Value.(int64)

	case FLOAT:
		res.Value = math.Ceil(a.Value.(float64))

	case BIGINT:
		res.Value = a.Value.(big.Int)

	case DEFAULT:
		return errors.Errorf("Wrong type %v encountered for fun ceil", a.Tid)
	}
	return nil
}

func applySince(a, res *types.Val) error {
	if a.Tid == types.TypeDatetime {
		a.Value = float64(time.Since(a.Value.(time.Time))) / 1000000000.0
		a.Tid = types.TypeFloat
		*res = *a
		return nil
	}
	return errors.Errorf("Wrong type %v encountered for func since", a.Tid)
}

type unaryFunc func(a, res *types.Val) error
type binaryFunc func(a, b, res *types.Val) error

var unaryFunctions = map[string]unaryFunc{
	"ln":    applyLn,
	"exp":   applyExp,
	"u-":    applyNeg,
	"sqrt":  applySqrt,
	"floor": applyFloor,
	"ceil":  applyCeil,
	"since": applySince,
}

var binaryFunctions = map[string]binaryFunc{
	"+":       applyAdd,
	"-":       applySub,
	"*":       applyMul,
	"/":       applyDiv,
	"%":       applyMod,
	"pow":     applyPow,
	"logbase": applyLog,
	"min":     applyMin,
	"max":     applyMax,
}

type valType int

const (
	INT valType = iota
	FLOAT
	BIGINT
	DEFAULT
)

func getValType(v *types.Val) valType {
	var vBase valType
	switch v.Tid {
	case types.TypeInt64:
		vBase = INT
	case types.TypeFloat:
		vBase = FLOAT
	case types.TypeBigInt:
		vBase = BIGINT
	default:
		vBase = DEFAULT
	}
	return vBase
}

func (ag *aggregator) matchType(v, va *types.Val) error {
	vBase := getValType(v)
	vaBase := getValType(va)
	if vBase == vaBase {
		return nil
	}

	if vBase == DEFAULT || vaBase == DEFAULT {
		return errors.Errorf("Wrong types %v, %v encontered for func %s", v.Tid,
			va.Tid, ag.name)
	}

	if vBase == INT && vaBase == BIGINT {
		v.Tid = types.TypeBigInt
		v.Value = big.NewInt(v.Value.(int64))
	}

	if vBase == INT && vaBase == FLOAT {
		v.Tid = types.TypeFloat
		v.Value = float64(v.Value.(int64))
	}

	if vBase == FLOAT && vaBase == BIGINT {
		v.Tid = types.TypeBigInt
		v.Value = big.NewInt(int64(v.Value.(float64)))
	}

	if vBase == FLOAT && vaBase == INT {
		va.Tid = types.TypeFloat
		va.Value = float64(v.Value.(int64))
	}

	if vBase == BIGINT && vaBase == INT {
		va.Tid = types.TypeBigInt
		va.Value = big.NewInt(v.Value.(int64))
	}

	if vBase == BIGINT && vaBase == FLOAT {
		va.Tid = types.TypeBigInt
		va.Value = big.NewInt(int64(v.Value.(float64)))
	}

	return nil
}

func (ag *aggregator) ApplyVal(v types.Val) error {
	if v.Value == nil {
		// If the value is missing, treat it as 0.
		v.Value = int64(0)
		v.Tid = types.TypeInt64
	}

	var res types.Val
	if function, ok := unaryFunctions[ag.name]; ok {
		res.Tid = v.Tid
		err := function(&v, &res)
		if err != nil {
			return err
		}
		ag.result = res
		return nil
	}

	if ag.result.Value == nil {
		ag.result = v
		return nil
	}

	va := ag.result
	if err := ag.matchType(&v, &va); err != nil {
		return err
	}

	if function, ok := binaryFunctions[ag.name]; ok {
		res.Tid = va.Tid
		err := function(&va, &v, &res)
		if err != nil {
			return err
		}
		ag.result = res
	} else {
		return errors.Errorf("Unhandled aggregator function %q", ag.name)
	}

	return nil
}

func (ag *aggregator) Apply(val types.Val) {
	if ag.result.Value == nil {
		ag.result = val
		ag.count++
		return
	}

	va := ag.result
	vb := val
	var res types.Val
	switch ag.name {
	case "min":
		r, err := types.Less(va, vb)
		if err == nil && !r {
			res = vb
		} else {
			res = va
		}
	case "max":
		r, err := types.Less(va, vb)
		if err == nil && r {
			res = vb
		} else {
			res = va
		}
	case "sum", "avg":
		switch {
		case va.Tid == types.TypeInt64 && vb.Tid == types.TypeInt64:
			va.Value = va.Value.(int64) + vb.Value.(int64)
		case va.Tid == types.TypeFloat && vb.Tid == types.TypeFloat:
			va.Value = va.Value.(float64) + vb.Value.(float64)
		case va.Tid == types.TypeBigInt && vb.Tid == types.TypeBigInt:
			a := va.Value.(big.Int)
			b := vb.Value.(big.Int)
			va.Value = *a.Add(&a, &b)
		}
		// Skipping the else case since that means the pair cannot be summed.
		res = va
	default:
		x.Fatalf("Unhandled aggregator function %v", ag.name)
	}
	ag.count++
	ag.result = res
}

func (ag *aggregator) ValueMarshalled() (*pb.TaskValue, error) {
	ag.divideByCount()
	if ag.result.Value == nil {
		return &pb.TaskValue{}, nil
	}
	src := ag.result
	data, err := types.ToBinary(src.Tid, src.Value)
	if err != nil {
		return &pb.TaskValue{}, err
	}
	return &pb.TaskValue{Val: data}, nil
}

func (ag *aggregator) divideByCount() {
	if ag.name != "avg" || ag.count == 0 || ag.result.Value == nil {
		return
	}
	var v float64
	switch ag.result.Tid {
	case types.TypeInt64:
		v = float64(ag.result.Value.(int64))
	case types.TypeFloat:
		v = ag.result.Value.(float64)
	case types.TypeBigInt:
		vi := ag.result.Value.(big.Int)
		vf := &big.Float{}
		vf = vf.SetInt(&vi)
		vq := big.NewFloat(float64(ag.count))

		ag.result.Tid = types.TypeFloat
		ag.result.Value, _ = vf.Quo(vf, vq).Float64()
		return
	}

	ag.result.Tid = types.TypeFloat
	ag.result.Value = v / float64(ag.count)
}

func (ag *aggregator) Value() (types.Val, error) {
	if ag.result.Value == nil {
		return ag.result, ErrEmptyVal
	}
	ag.divideByCount()
	if ag.result.Tid == types.TypeFloat {
		switch {
		case math.IsInf(ag.result.Value.(float64), 1):
			ag.result.Value = math.MaxFloat64
		case math.IsInf(ag.result.Value.(float64), -1):
			ag.result.Value = -1 * math.MaxFloat64
		case math.IsNaN(ag.result.Value.(float64)):
			ag.result.Value = 0.0
		}
	}
	return ag.result, nil
}
