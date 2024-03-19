package goredis

import (
	"fmt"
	"io"
)

func readObject(r io.Reader) (any, error) {
	b, err := readOne(r)
	if err != nil {
		return nil, err
	}

	switch b {
	case '*':
		return readArray(r, false)
	case '$':
		return readBulkString(r)
	default:
		return nil, fmt.Errorf("unrecognized character %c", b)
	}
}

func readBulkString(r io.Reader) (string, error) {
	size, err := readLength(r)
	if err != nil {
		return "", err
	}

	buff := make([]byte, size)
	if _, err := io.ReadFull(r, buff); err != nil {
		return "", err
	}

	b, err := readOne(r)
	if err != nil {
		return "", err
	}
	if b != '\r' {
		return "", fmt.Errorf("expected carriage-return character for array, but found %c", b)
	}

	b, err = readOne(r)
	if err != nil {
		return "", err
	}
	if b != '\n' {
		return "", fmt.Errorf("expected newline character for array, but found %c", b)
	}

	return string(buff), nil
}

func readArray(r io.Reader, readFirstChar bool) ([]any, error) {
	if readFirstChar {
		b, err := readOne(r)
		if err != nil {
			return nil, err
		}
		if b != '*' {
			return nil, fmt.Errorf("expected * character for array, but found %c", b)
		}
	}

	n, err := readLength(r)
	if err != nil {
		return nil, err
	}

	result := make([]any, n)
	for i := 0; i < n; i++ {
		val, err := readObject(r)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}

	return result, nil
}

func readOne(r io.Reader) (byte, error) {
	buff := [1]byte{}
	n, err := r.Read(buff[:])
	if err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, io.EOF
	}
	return buff[0], nil
}

func readLength(r io.Reader) (int, error) {
	result := 0
	for {
		b, err := readOne(r)
		if err != nil {
			return 0, err
		}
		if b == '\r' {
			break
		}
		result = result*10 + int(b-'0')
	}
	b, err := readOne(r)
	if err != nil {
		return 0, err
	}
	if b != '\n' {
		return 0, fmt.Errorf("expected newline character for length, but found %c", b)
	}
	return result, nil
}
