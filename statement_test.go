package gdo

import (
	"database/sql"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStatement(t *testing.T) {

	cases := []map[string]interface{}{
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1=1", "isParameterized": false},
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1=?", "isParameterized": false},
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1=:id:", "isParameterized": true},
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1 = :id:", "isParameterized": true},
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1=id:", "isParameterized": false},
		map[string]interface{}{"query": "SELECT * FROM Foo WHERE 1 = :id", "isParameterized": false},
	}

	var namesArgs []sql.NamedArg
	var args []interface{}

	for _, c := range cases {
		expected := &Statement{
			query:           c["query"].(string),
			namedArgs:       namesArgs,
			args:            args,
			isParameterized: c["isParameterized"].(bool),
		}

		assert.Equal(t, expected, NewStatement(c["query"].(string)))
	}
}

func TestProcessStatement(t *testing.T) {
	a := rand.Int63()
	b := rand.Int63()

	cases := []map[string]interface{}{
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :a: AND bar = :b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = ? AND bar = ?",
			"args":          []interface{}{a, b},
			"error":         nil,
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :b: AND bar = :a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = ? AND bar = ?",
			"args":          []interface{}{b, a},
			"error":         nil,
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:a: AND bar=:b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=?",
			"args":          []interface{}{a, b},
			"error":         nil,
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b: AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=?",
			"args":          []interface{}{b, a},
			"error":         nil,
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:a AND bar=:b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=:a AND bar=?",
			"args":          []interface{}{b},
			"error":         errors.New("gdo: you have a parameter mismatch"),
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=:b AND bar=?",
			"args":          []interface{}{a},
			"error":         errors.New("gdo: you have a parameter mismatch"),
		},
	}

	for _, c := range cases {
		expected := &Statement{
			query:     c["expectedQuery"].(string),
			args:      c["args"].([]interface{}),
			namedArgs: []sql.NamedArg{sql.Named("a", a), sql.Named("b", b)},
		}

		stmt := NewStatement(c["query"].(string))
		stmt.BindNamedArg(sql.Named("a", a))
		stmt.BindNamedArg(sql.Named("b", b))

		newStmt, err := processStatment(stmt)

		if err != nil {
			assert.Equal(t, c["error"], err)
			assert.Nil(t, newStmt)
		} else {
			assert.Equal(t, expected, newStmt)
			assert.NoError(t, err)
		}
	}
}

func TestLastExecutedQuery(t *testing.T) {
	a := string(rand.Intn(255))
	b := string(rand.Intn(255))

	cases := []map[string]interface{}{
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :a: AND bar = :b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = '" + a + "' AND bar = '" + b + "'",
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :b: AND bar = :a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = '" + b + "' AND bar = '" + a + "'",
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:a: AND bar=:b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id='" + a + "' AND bar='" + b + "'",
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b: AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id='" + b + "' AND bar='" + a + "'",
		},
	}

	for _, c := range cases {
		stmt := NewStatement(c["query"].(string))
		stmt.BindNamedArg(sql.Named("a", a))
		stmt.BindNamedArg(sql.Named("b", b))

		newStmt, err := processStatment(stmt)

		assert.Equal(t, c["expectedQuery"].(string), newStmt.lastExecutedQuery())
		assert.NoError(t, err)
	}
}
