package gdo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckIsParameterized(t *testing.T) {

	cases := []string{
		"SELECT * FROM Foo WHERE f = :id:",
		"SELECT * FROM Foo WHERE f = 'id'",
		"SELECT * FROM Foo",
		"SELECT * FROM Foo WHERE f = :id :",
		"SELECT * FROM Foo WHERE f = : id:",
		"SELECT * FROM Foo WHERE f = : id :",
		"SELECT * FROM Foo WHERE f=:id:",
		"SELECT * FROM Foo WHERE f='id'",
		"SELECT * FROM Foo WHERE f=:id :",
		"SELECT * FROM Foo WHERE f=: id:",
		"SELECT * FROM Foo WHERE f=: id :",
		"SELECT * FROM Foo WHERE f=:id  :",
		"SELECT * FROM Foo WHERE f=:  id:",
		"SELECT * FROM Foo WHERE f=:  id  :",
	}

	expected := []bool{
		true,
		false,
		false,
		false,
		false,
		false,
		true,
		false,
		false,
		false,
		false,
		false,
		false,
		false,
		false,
	}

	for k, c := range cases {
		assert.Equal(t, expected[k], checkIsParameterized(c))
	}
}
