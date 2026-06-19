package middleware

import (
	"net/http"

	"traceid-demo/trace"
)

func TraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, traceID := trace.ExtractOrNew(r.Context(), r.Header)
		r = r.WithContext(ctx)
		w.Header().Set(trace.HeaderTraceID, traceID)
		next.ServeHTTP(w, r)
	})
}
