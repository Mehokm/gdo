package gdo

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"
)

var ErrColNotFound = errors.New("gdo: column not found")
var ErrCannotConvert = errors.New("gdo: cannot convert value to type")

type Rows []Row
type Row map[string]interface{}

func (r Row) Int(col string) (int, error) {
	val, ok := r[col]

	if !ok {
		return 0, ErrColNotFound
	}

	var v int

	switch val.(type) {
	case int8:
		v = int(val.(int8))
	case int16:
		v = int(val.(int16))
	case int32:
		v = int(val.(int32))
	case int64:
		v = int(val.(int64))
	case float32:
		v = int(val.(float32))
	case float64:
		v = int(val.(float64))
	case []byte:
		s, err := strconv.Atoi(string(val.([]byte)))

		v = s

		if err != nil {
			v = 0
		}
	default:
		return 0, ErrCannotConvert
	}

	return v, nil
}

func (r Row) String(col string) (string, error) {
	val, ok := r[col]

	if !ok {
		return "", ErrColNotFound
	}

	var v string

	switch val.(type) {
	case int8:
		v = string(val.(int8))
	case int16:
		v = string(val.(int16))
	case int32:
		v = string(val.(int32))
	case int64:
		v = string(val.(int64))
	case float32:
		v = strconv.FormatFloat(float64(val.(float32)), 'f', -1, 32)
	case float64:
		v = strconv.FormatFloat(val.(float64), 'f', -1, 64)
	case []byte:
		v = string(val.([]byte))
	default:
		return "", ErrCannotConvert
	}

	return v, nil
}

func (r Row) Float64(col string) (float64, error) {
	val, ok := r[col]

	if !ok {
		return 0, ErrColNotFound
	}

	var v float64

	switch val.(type) {
	case int8:
		v = float64(val.(int8))
	case int16:
		v = float64(val.(int16))
	case int32:
		v = float64(val.(int32))
	case int64:
		v = float64(val.(int64))
	case float32:
		v = float64(val.(float32))
	case float64:
		v = val.(float64)
	case []byte:
		v = math.Float64frombits(binary.LittleEndian.Uint64(val.([]byte)))
	default:
		return 0, ErrCannotConvert
	}

	return v, nil
}

func (r Row) Float32(col string) (float32, error) {
	val, ok := r[col]

	if !ok {
		return 0, ErrColNotFound
	}

	var v float32

	switch val.(type) {
	case int8:
		v = float32(val.(int8))
	case int16:
		v = float32(val.(int16))
	case int32:
		v = float32(val.(int32))
	case int64:
		v = float32(val.(int64))
	case float32:
		v = val.(float32)
	case float64:
		v = float32(val.(float64))
	case []byte:
		v = math.Float32frombits(binary.LittleEndian.Uint32(val.([]byte)))
	default:
		return 0, ErrCannotConvert
	}

	return v, nil
}

func (r Row) Bool(col string) (bool, error) {
	val, ok := r[col]

	if !ok {
		return false, ErrColNotFound
	}

	var v bool

	switch val.(type) {
	case int8:
		v = val.(int8) != 0
	case int16:
		v = val.(int16) != 0
	case int32:
		v = val.(int32) != 0
	case int64:
		v = val.(int64) != 0
	case float32:
		v = val.(float32) != 0
	case float64:
		v = val.(float64) != 0
	case []byte:
		v = val.([]byte)[0] != 0
	default:
		return false, ErrCannotConvert
	}

	return v, nil
}

func (r Row) Bytes(col string) ([]byte, error) {
	val, ok := r[col]

	if !ok {
		return nil, ErrColNotFound
	}

	var v []byte

	switch val.(type) {
	case []byte:
		v = val.([]byte)
	default:
		return nil, ErrCannotConvert
	}

	return v, nil
}

func (r Row) Time(col string) (time.Time, error) {
	val, ok := r[col]

	if !ok {
		return time.Time{}, ErrColNotFound
	}

	var v time.Time
	var err error

	switch val.(type) {
	case []byte:
		vv := string(val.([]byte))

		v, err = time.Parse(getTimeFormat(vv), vv)

		if err != nil {
			return v, err
		}
	default:
		return time.Time{}, ErrCannotConvert
	}

	return v, nil
}

func getTimeFormat(s string) string {
	// mysql format using go specific numbers
	format := "2006-01-02 15:04:05.999999"

	if len(s) > len(format) {
		return ""
	}

	return format[:len(s)]
}
