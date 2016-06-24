package logger

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNominal(t *testing.T) {
	buf := bytes.Buffer{}
	logger.Out = &buf
	Init("info")
	l := GetCapturedLogger()
	l.Print("test")
	result := make(map[string]string)
	_ = json.Unmarshal(buf.Bytes(), &result)
	assert.Equal(t, "test", result["msg"])
	assert.Equal(t, "capturer_test.go:17", result["caller"])
	assert.Equal(t, "warning", result["level"])
	assert.NotEqual(t, "", result["fields.time"])
	_, err := time.Parse(timestampFormat, result["time"])
	assert.Equal(t, err, nil)
}

func TestLarge(t *testing.T) {
	buf := bytes.Buffer{}
	logger.Out = &buf
	Init("info")
	l := GetCapturedLogger()
	manyA := make([]rune, 10000)
	for i := range manyA {
		manyA[i] = 'A'
	}
	aString := string(manyA)
	l.Print(aString)
	result := make(map[string]string)
	_ = json.Unmarshal(buf.Bytes(), &result)
	assert.Equal(t, aString, result["msg"])
}
