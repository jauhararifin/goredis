package goredis

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type messageReader struct {
	r             *bufio.Reader
	temp          []byte
	stringBuilder strings.Builder
}

func newMessageReader(r io.Reader) *messageReader {
	return &messageReader{
		r:             bufio.NewReader(r),
		temp:          make([]byte, 4096),
		stringBuilder: strings.Builder{},
	}
}

func (m *messageReader) ReadArrayLen() (length int, err error) {
	b, err := m.r.ReadByte()
	if err != nil {
		return 0, err
	}

	if b != '*' {
		return 0, fmt.Errorf("malformed input")
	}

	length, err = m.readValueLength()
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (m *messageReader) readValueLength() (int, error) {
	b, err := m.r.ReadByte()
	if err != nil {
		return 0, err
	}

	if b < '0' || b > '9' {
		return 0, fmt.Errorf("malformed input")
	}
	result := int(b - '0')

	for b != '\r' {
		b, err = m.r.ReadByte()
		if err != nil {
			return 0, err
		}

		if b != '\r' {
			if b < '0' || b > '9' {
				return 0, fmt.Errorf("malformed input")
			}
			result = result*10 + int(b-'0')
		}
	}

	b, err = m.r.ReadByte()
	if err != nil {
		return 0, err
	}
	if b != '\n' {
		return 0, fmt.Errorf("malformed input")
	}

	return result, nil
}

func (m *messageReader) readUntilCRLF(w io.Writer) error {
	b, err := m.r.ReadByte()
	if err != nil {
		return err
	}

	for b != '\r' {
		if _, err := w.Write([]byte{b}); err != nil {
			return err
		}

		b, err = m.r.ReadByte()
		if err != nil {
			return err
		}
	}

	b, err = m.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\n' {
		return fmt.Errorf("malformed input")
	}

	return nil
}

func (m *messageReader) skipCRLF() error {
	b, err := m.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\r' {
		return fmt.Errorf("malformed input")
	}

	b, err = m.r.ReadByte()
	if err != nil {
		return err
	}
	if b != '\n' {
		return fmt.Errorf("malformed input")
	}

	return nil
}

func (m *messageReader) ReadString() (string, error) {
	b, err := m.r.ReadByte()
	if err != nil {
		return "", err
	}

	if b == '$' {
		length, err := m.readValueLength()
		if err != nil {
			return "", err
		}

		m.stringBuilder.Reset()
		m.stringBuilder.Grow(length)
		if err := m.pipeToWriter(&m.stringBuilder, length); err != nil {
			return "", err
		}

		s := m.stringBuilder.String()
		if err := m.skipCRLF(); err != nil {
			return "", err
		}

		return s, nil
	} else if b == '+' {
		m.stringBuilder.Reset()
		if err := m.readUntilCRLF(&m.stringBuilder); err != nil {
			return "", err
		}
		return m.stringBuilder.String(), nil
	} else {
		return "", fmt.Errorf("malformed input")
	}
}

func (m *messageReader) pipeToWriter(w io.Writer, length int) error {
	remaining := length
	for remaining > 0 {
		n := remaining
		if n > len(m.temp) {
			n = len(m.temp)
		}

		if _, err := io.ReadFull(m.r, m.temp[:n]); err != nil {
			return err
		}
		remaining -= n

		if _, err := w.Write(m.temp[:n]); err != nil {
			return err
		}
	}

	return nil
}
