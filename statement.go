package gdo

import (
	"database/sql"
	"errors"
	"index/suffixarray"
	"sort"
	"strconv"
	"strings"
)

var ErrParameterMismatch = errors.New("gdo: you have a parameter mismatch")

type Statement struct {
	query          string
	namedArgs      []sql.NamedArg
	args           []interface{}
	isParamertized bool
}

// NewStatement returns a Statement
func NewStatement(query string) *Statement {
	var namedArgs []sql.NamedArg
	var args []interface{}

	isParamertized := !strings.Contains(query, "?")

	return &Statement{
		query:          query,
		namedArgs:      namedArgs,
		args:           args,
		isParamertized: isParamertized,
	}
}

func (stmt *Statement) BindNamedArgs(namedArgs []sql.NamedArg) {
	stmt.namedArgs = namedArgs
}

func (stmt *Statement) BindNamedArg(namedArg sql.NamedArg) {
	stmt.namedArgs = append(stmt.namedArgs, namedArg)
}

func (stmt *Statement) BindArgs(args []interface{}) {
	stmt.args = args
}

func (stmt *Statement) BindArg(arg interface{}) {
	stmt.args = append(stmt.args, arg)
}

func (stmt *Statement) lastExecutedQuery() string {
	lastExecQuery := stmt.query

	if len(stmt.args) > 0 {
		index := suffixarray.New([]byte(stmt.query))
		inds := index.Lookup([]byte("?"), -1)

		sort.Ints(inds)

		interQuery := stmt.query

		var padding int
		for i, ind := range inds {
			var s string

			arg := stmt.args[i]

			switch arg.(type) {
			case int:
				s = strconv.Itoa(arg.(int))
			case float32:
				s = strconv.FormatFloat(float64(arg.(float32)), 'f', -1, 32)
			case float64:
				s = strconv.FormatFloat(arg.(float64), 'f', -1, 64)
			case string:
				s = "'" + arg.(string) + "'"
			case nil:
				s = "NULL"
			}

			interQuery = insertAt(interQuery, s, ind+padding-i)

			padding += len(s)
		}

		lastExecQuery = interQuery
	}

	return lastExecQuery
}

func processStatment(s *Statement) (*Statement, error) {
	index := suffixarray.New([]byte(s.query))

	indexMap := make(map[int]sql.NamedArg)

	var indicies []int
	var args []interface{}
	var toReplace []string

	var argCount int
	for _, arg := range s.namedArgs {
		argName := delim + arg.Name + delim

		inds := index.Lookup([]byte(argName), -1)

		if len(inds) <= 0 {
			break
		}

		for _, ind := range inds {
			indexMap[ind] = arg

			indicies = append(indicies, ind)
		}

		toReplace = append(toReplace, argName, "?")

		argCount++
	}

	if argCount != len(s.namedArgs) {
		return nil, ErrParameterMismatch
	}

	sort.Ints(indicies)

	for _, ind := range indicies {
		args = append(args, indexMap[ind].Value)
	}

	replacedSQL := strings.NewReplacer(toReplace...).Replace(s.query)

	return &Statement{
		query:     replacedSQL,
		namedArgs: s.namedArgs,
		args:      args,
	}, nil
}

func insertAt(str, toIns string, pos int) string {
	return str[:pos] + toIns + str[pos+1:]
}
