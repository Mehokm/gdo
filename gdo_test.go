package gdo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestPrepareContext(t *testing.T) {
	cases := []map[string]interface{}{
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :a: AND bar = :b: AND foo = :a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = ? AND bar = ? AND foo = ?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{0, 2},
					"b": []int{1},
				},
				total: 3,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id = :b: AND bar = :a: AND foo = :b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id = ? AND bar = ? AND foo = ?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{1},
					"b": []int{0, 2},
				},
				total: 3,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:a: AND bar=:b:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{0},
					"b": []int{1},
				},
				total: 2,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b: AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{1},
					"b": []int{0},
				},
				total: 2,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b: AND bar=:a",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=:a",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"b": []int{0},
				},
				total: 1,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=:b AND bar=?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{0},
				},
				total: 1,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=:b: AND bar=a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=? AND bar=a:",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"b": []int{0},
				},
				total: 1,
			},
		},
		map[string]interface{}{
			"query":         "SELECT * FROM Foo WHERE id=b: AND bar=:a:",
			"expectedQuery": "SELECT * FROM Foo WHERE id=b: AND bar=?",
			"queryNamedArgs": queryNamedArgs{
				dict: map[string][]int{
					"a": []int{0},
				},
				total: 1,
			},
		},
	}

	for _, c := range cases {
		db, mock, _ := sqlmock.New()
		defer db.Close()

		mock.ExpectPrepare("SELECT *")

		g := New(db)

		pStmt, err := g.prepareContext(context.Background(), c["query"].(string))

		assert.Equal(t, c["expectedQuery"].(string), pStmt.Statement.query)
		assert.Equal(t, c["queryNamedArgs"], pStmt.queryNamedArgs)
		assert.NoError(t, err)
	}
}

func TestCheckIsParameterized(t *testing.T) {

	cases := []map[string]interface{}{
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id:",
			"expected": true,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = 'id'",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = : id:",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = : id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:id:",
			"expected": true,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f='id'",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=: id:",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=: id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=: id :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:id  :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:  id:",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:  id  :",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:id ",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=: id",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=: id ",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:id  ",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:  id",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f=:  id  ",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id: AND g = :foo",
			"expected": true,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id AND g = :foo:",
			"expected": true,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id: AND g = :foo:",
			"expected": true,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = :id AND g = :foo",
			"expected": false,
		},
		map[string]interface{}{
			"case":     "SELECT * FROM Foo WHERE f = id:",
			"expected": false,
		},
	}

	for _, c := range cases {
		assert.Equal(t, c["expected"].(bool), checkIsParameterized(c["case"].(string)), c["case"].(string))
	}
}
