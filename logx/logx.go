package logx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"traceid-demo/trace"
)

type Entry struct {
	TraceID   string         `json:"trace_id"`
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Service   string         `json:"service"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type Logger struct {
	service string
	output  io.Writer
	store   Store
	mu      sync.Mutex
}

type Store interface {
	Append(entry Entry)
	QueryByTraceID(traceID string) []Entry
}

var defaultLogger *Logger

func init() {
	defaultLogger = New("unknown", os.Stdout, nil)
}

func New(service string, output io.Writer, store Store) *Logger {
	return &Logger{
		service: service,
		output:  output,
		store:   store,
	}
}

func Init(service string, output io.Writer, store Store) {
	defaultLogger = New(service, output, store)
}

func (l *Logger) log(ctx context.Context, level, msg string, fields map[string]any) {
	entry := Entry{
		TraceID:   trace.FromContext(ctx),
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     level,
		Service:   l.service,
		Message:   msg,
		Fields:    fields,
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.output != nil {
		data, _ := json.Marshal(entry)
		fmt.Fprintln(l.output, string(data))
	}

	if l.store != nil {
		l.store.Append(entry)
	}
}

func (l *Logger) Info(ctx context.Context, msg string, fields ...map[string]any) {
	var f map[string]any
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(ctx, "INFO", msg, f)
}

func (l *Logger) Error(ctx context.Context, msg string, fields ...map[string]any) {
	var f map[string]any
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(ctx, "ERROR", msg, f)
}

func (l *Logger) Warn(ctx context.Context, msg string, fields ...map[string]any) {
	var f map[string]any
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(ctx, "WARN", msg, f)
}

func (l *Logger) Debug(ctx context.Context, msg string, fields ...map[string]any) {
	var f map[string]any
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(ctx, "DEBUG", msg, f)
}

func Info(ctx context.Context, msg string, fields ...map[string]any) {
	defaultLogger.Info(ctx, msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...map[string]any) {
	defaultLogger.Error(ctx, msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...map[string]any) {
	defaultLogger.Warn(ctx, msg, fields...)
}

func Debug(ctx context.Context, msg string, fields ...map[string]any) {
	defaultLogger.Debug(ctx, msg, fields...)
}

func Fatal(v ...any) {
	log.Fatal(v...)
}
