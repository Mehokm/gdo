package gdo

import (
	"database/sql"
	"index/suffixarray"
	"sort"
	"strconv"
	"strings"
)

type Statement struct {
	query      string
	interQuery string
	namedArgs  []sql.NamedArg
	args       []interface{}
}

// NewStatement returns a Statement
func NewStatement(query string) *Statement {
	var namedArgs []sql.NamedArg
	var args []interface{}

	return &Statement{
		query:      query,
		interQuery: "",
		namedArgs:  namedArgs,
		args:       args,
	}
}

func (s *Statement) BindParams(namedArgs []sql.NamedArg) {
	s.namedArgs = namedArgs
}

func processStatment(s *Statement) *Statement {
	index := suffixarray.New([]byte(s.query))

	indexMap := make(map[int]sql.NamedArg)

	var indicies []int
	var args []interface{}
	var toReplace []string

	for _, arg := range s.namedArgs {
		argName := "@" + arg.Name

		inds := index.Lookup([]byte(argName), -1)

		for _, ind := range inds {
			indexMap[ind] = arg
		}

		indicies = append(indicies, inds...)

		toReplace = append(toReplace, argName, "?")
	}

	sort.Ints(indicies)

	for _, ind := range indicies {
		args = append(args, indexMap[ind].Value)
	}

	replacedSQL := strings.NewReplacer(toReplace...).Replace(s.query)

	index = suffixarray.New([]byte(replacedSQL))
	inds := index.Lookup([]byte("?"), -1)

	sort.Ints(inds)

	interQuery := replacedSQL

	var padding int
	for i, ind := range inds {
		var s string

		switch args[i].(type) {
		case int:
			s = strconv.Itoa(args[i].(int))
		case string:
			s = "'" + args[i].(string) + "'"
		case nil:
			s = "NULL"
		}

		interQuery = insertAt(interQuery, s, ind+padding-i)

		padding += len(s)
	}

	return &Statement{
		query:      replacedSQL,
		namedArgs:  s.namedArgs,
		args:       args,
		interQuery: interQuery,
	}
}
