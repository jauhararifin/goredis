package goredis

import (
	"strings"
	"testing"
)

func TestReadResp(t *testing.T) {
	input := "*3\r\n$5\r\nword1\r\n$5\r\nword2\r\n$5\r\nword3\r\n"
	reader := strings.NewReader(input)
	result, err := readArray(reader, true)
	if err != nil {
		t.Error("cannot parse input: %w", err)
		return
	}

	if len(result) != 3 {
		t.Error("result is not an array with size 3")
		return
	}

	element0, ok := result[0].(string)
	if !ok {
		t.Error(`result[0] is not "word1"`)
		return
	}
	if element0 != "word1" {
		t.Error(`result[0] is not "word1"`)
		return
	}

	element1, ok := result[1].(string)
	if !ok {
		t.Error(`result[1] is not "word2"`)
		return
	}
	if element1 != "word2" {
		t.Error(`result[1] is not "word2"`)
		return
	}

	element2, ok := result[2].(string)
	if !ok {
		t.Error(`result[2] is not "word3"`)
		return
	}
	if element2 != "word3" {
		t.Error(`result[2] is not "word3"`)
		return
	}
}
