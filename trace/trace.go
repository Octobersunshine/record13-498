package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

const (
	HeaderTraceID = "X-Trace-ID"
)

type traceIDKey struct{}

func New() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fallbackTraceID()
	}
	return hex.EncodeToString(b)
}

func fallbackTraceID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return hex.EncodeToString(b)
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

func FromContext(ctx context.Context) string {
	if id, ok := ctx.Value(traceIDKey{}).(string); ok {
		return id
	}
	return ""
}

func FromHeader(h http.Header) string {
	return h.Get(HeaderTraceID)
}

func SetHeader(h http.Header, traceID string) {
	h.Set(HeaderTraceID, traceID)
}

func ExtractOrNew(ctx context.Context, h http.Header) (context.Context, string) {
	traceID := FromHeader(h)
	if traceID == "" {
		traceID = New()
	}
	return WithTraceID(ctx, traceID), traceID
}
