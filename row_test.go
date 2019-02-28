package gdo

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowInt(t *testing.T) {
	c := make(Row)
	c["int8"] = int8(1)
	c["int16"] = int16(1)
	c["int32"] = int32(1)
	c["int64"] = int64(1)
	c["float32"] = float32(1)
	c["float64"] = float64(1)
	c["byte"] = []byte(strconv.Itoa(1))
	c["err1"] = "foo"
	c["err2"] = []byte{10}

	var err error
	var expected int

	expected, err = c.Int("int8")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("int16")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("int32")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("int64")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("float32")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("float64")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("byte")
	assert.Equal(t, 1, expected)
	assert.NoError(t, err)

	expected, err = c.Int("err1")
	assert.Equal(t, 0, expected)
	assert.Equal(t, ErrCannotConvert, err)

	expected, err = c.Int("err2")
	assert.Equal(t, 0, expected)
	assert.Equal(t, ErrCannotConvert, err)
}
