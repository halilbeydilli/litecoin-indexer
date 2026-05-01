// Package logger provides a simple structured logger with timestamps and levels.
// Zero external dependencies.
package logger

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Level represents the severity of a log message.
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

var levelNames = map[Level]string{
	DEBUG: "DEBUG",
	INFO:  " INFO",
	WARN:  " WARN",
	ERROR: "ERROR",
}

var levelColors = map[Level]string{
	DEBUG: "\033[90m", // gray
	INFO:  "\033[36m", // cyan
	WARN:  "\033[33m", // yellow
	ERROR: "\033[31m", // red
}

const reset = "\033[0m"

var (
	currentLevel Level = INFO
	mu           sync.Mutex
)

// SetLevel sets the minimum logging level.
func SetLevel(l Level) {
	mu.Lock()
	currentLevel = l
	mu.Unlock()
}

// Log writes a structured log message at the given level.
func Log(level Level, msg string, fields ...map[string]interface{}) {
	mu.Lock()
	cl := currentLevel
	mu.Unlock()

	if level < cl {
		return
	}

	ts := time.Now().Format("2006-01-02 15:04:05.000")
	color := levelColors[level]
	name := levelNames[level]
	prefix := fmt.Sprintf("%s[%s] [%s]%s", color, ts, name, reset)

	if len(fields) > 0 && len(fields[0]) > 0 {
		parts := make([]string, 0, len(fields[0]))
		for k, v := range fields[0] {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		fmt.Fprintf(os.Stdout, "%s %s | %s\n", prefix, msg, strings.Join(parts, " "))
	} else {
		fmt.Fprintf(os.Stdout, "%s %s\n", prefix, msg)
	}
}

// F creates a field map for structured logging. Convenience helper.
func F(keysAndValues ...interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keysAndValues[i])
		}
		m[key] = keysAndValues[i+1]
	}
	return m
}

// Convenience functions

func Debugf(msg string, fields ...map[string]interface{}) { Log(DEBUG, msg, fields...) }
func Infof(msg string, fields ...map[string]interface{})  { Log(INFO, msg, fields...) }
func Warnf(msg string, fields ...map[string]interface{})  { Log(WARN, msg, fields...) }
func Errorf(msg string, fields ...map[string]interface{}) { Log(ERROR, msg, fields...) }
