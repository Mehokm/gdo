package gdo

import (
	"database/sql"
	"index/suffixarray"
	"sort"
	"strings"
)

type GDO struct {
	db *sql.DB
}

type statement struct {
	rawSQL  string
	SQL     string
	rawArgs []sql.NamedArg
	args    []interface{}
}

type QueryResult struct {
	rows *sql.Rows
	cols []string
}

type ExecResult struct {
	sql.Result
}

type Map []map[string]interface{}

func New(db *sql.DB) GDO {
	return GDO{db}
}

// Statement returns a statement
func Statement(s string) *statement {
	var rawArgs []sql.NamedArg
	var args []interface{}

	return &statement{s, "", rawArgs, args}
}

func (s *statement) BindParams(rawArgs []sql.NamedArg) {
	s.rawArgs = rawArgs
}

func (g GDO) Exec(s *statement) (ExecResult, error) {
	var result sql.Result
	var err error

	if len(s.rawArgs) > 0 {
		newS := g.processStatment(s)

		result, err = g.db.Exec(newS.SQL, newS.args...)
	} else {
		result, err = g.db.Exec(s.rawSQL)
	}

	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{result}, nil
}

func (g GDO) Query(s *statement) (QueryResult, error) {
	var rows *sql.Rows
	var err error

	if len(s.rawArgs) > 0 {
		newS := g.processStatment(s)

		rows, err = g.db.Query(newS.SQL, newS.args...)
	} else {
		rows, err = g.db.Query(s.rawSQL)
	}

	if err != nil {
		return QueryResult{}, err
	}

	cols, err := rows.Columns()

	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{rows, cols}, nil
}

func (g GDO) processStatment(s *statement) *statement {
	index := suffixarray.New([]byte(s.rawSQL))

	indexMap := make(map[int]sql.NamedArg)

	var indicies []int
	var args []interface{}
	var toReplace []string

	for _, arg := range s.rawArgs {
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

	replacer := strings.NewReplacer(toReplace...)
	replacedSQL := replacer.Replace(s.rawSQL)

	return &statement{
		rawSQL:  s.rawSQL,
		SQL:     replacedSQL,
		rawArgs: s.rawArgs,
		args:    args,
	}
}

func (r QueryResult) FetchMap() Map {
	var m Map

	result := make([]sql.RawBytes, len(r.cols))
	rawResult := make([]interface{}, len(r.cols))

	for i := range rawResult {
		rawResult[i] = &result[i]
	}

	defer r.rows.Close()

	for r.rows.Next() {
		assoc := make(map[string]interface{})

		r.rows.Scan(rawResult...)

		for i := range result {
			assoc[r.cols[i]] = string(result[i])
		}

		m = append(m, assoc)
	}

	return m
}
