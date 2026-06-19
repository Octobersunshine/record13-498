package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"traceid-demo/httpclient"
	"traceid-demo/middleware"
	"traceid-demo/trace"
)

func main() {
	go startDownstreamServer(":9091")

	mux := http.NewServeMux()

	mux.Handle("/api/call-downstream", middleware.TraceID(http.HandlerFunc(handleCallDownstream)))
	mux.Handle("/api/hello", middleware.TraceID(http.HandlerFunc(handleHello)))

	log.Println("上游服务启动于 :9090")
	if err := http.ListenAndServe(":9090", mux); err != nil {
		log.Fatal(err)
	}
}

func handleHello(w http.ResponseWriter, r *http.Request) {
	traceID := trace.FromContext(r.Context())
	fmt.Fprintf(w, "Hello, TraceID: %s\n", traceID)
}

func handleCallDownstream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	upstreamTraceID := trace.FromContext(ctx)

	client := httpclient.Default()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:9091/downstream/echo", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := map[string]any{
		"upstream_trace_id":   upstreamTraceID,
		"downstream_trace_id": resp.Header.Get(trace.HeaderTraceID),
		"downstream_body":    string(body),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func startDownstreamServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/downstream/echo", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := trace.FromContext(r.Context())
		received := r.Header.Get(trace.HeaderTraceID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"received_trace_id":     received,
			"context_trace_id":    traceID,
			"message":            "下游服务处理成功",
		})
	})))

	log.Printf("下游服务启动于 %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("下游服务启动失败: %v", err)
	}
}
