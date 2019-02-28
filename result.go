package gdo

import (
	"database/sql"
	"reflect"
	"strings"
)

type GDOResult struct {
	executedStmt *Statement
}

type ExecResult struct {
	GDOResult
	sql.Result
}

type QueryResult struct {
	GDOResult
	Rows *sql.Rows
	Cols []string
}

type QueryRowResult struct {
	QueryResult
	err error
}

func (r QueryResult) FetchRows() Rows {
	var m Rows

	results := make([]interface{}, len(r.Cols))
	rawResults := make([]interface{}, len(r.Cols))

	for i := range rawResults {
		rawResults[i] = &results[i]
	}

	defer r.Rows.Close()

	for r.Rows.Next() {
		assoc := make(map[string]interface{})

		r.Rows.Scan(rawResults...)

		for i := range results {
			assoc[r.Cols[i]] = results[i]
		}

		m = append(m, assoc)
	}

	return m
}

func (qrr QueryRowResult) FetchRow() Row {
	var r Row

	if qrr.Rows == nil {
		return r
	}

	rs := qrr.FetchRows()

	if len(rs) < 1 {
		return r
	}

	return rs[0]
}

func (qr QueryResult) FetchRowsTyped(t interface{}) (interface{}, error) {
	stype := reflect.TypeOf(t).Elem()

	slice := newTypedSlice(stype)

	newStruct := newTypedStruct(stype)

	cols, err := qr.Rows.Columns()

	if err != nil {
		return nil, err
	}

	ptrs := make([]interface{}, len(cols))

	fieldsLen := newStruct.NumField()
	for i := 0; i < len(cols); i++ {
		ptrs[i] = new(interface{})

		if i < fieldsLen {
			field := newStruct.Field(i)

			if field.CanAddr() && isValidField(stype.Field(i), cols[i]) {
				ptrs[i] = field.Addr().Interface()
			}
		}
	}

	defer qr.Rows.Close()

	for qr.Rows.Next() {
		err := qr.Rows.Scan(ptrs...)

		if err != nil {
			return nil, err
		}

		slice.Set(reflect.Append(slice, newStruct))
	}

	return slice.Interface(), nil
}

func (qrr QueryRowResult) LastError() error {
	return qrr.err
}

func (r GDOResult) LastExecutedQuery() string {
	return r.executedStmt.lastExecutedQuery()
}

// HELPERS
func newTypedSlice(t reflect.Type) reflect.Value {
	sliceValue := reflect.MakeSlice(reflect.SliceOf(t), 0, 0)

	slice := reflect.New(sliceValue.Type()).Elem()
	slice.Set(sliceValue)

	return slice
}

func newTypedStruct(t reflect.Type) reflect.Value {
	return reflect.New(t).Elem()
}

func isValidField(field reflect.StructField, column string) bool {
	validName := field.Name == strings.Title(column)
	validTag := field.Tag.Get("gdo") == column

	return validName || validTag
}
