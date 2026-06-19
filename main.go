package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"traceid-demo/async"
	"traceid-demo/httpclient"
	"traceid-demo/middleware"
	"traceid-demo/trace"
)

func main() {
	go startDownstreamServer(":9091")

	mux := http.NewServeMux()

	mux.Handle("/api/call-downstream", middleware.TraceID(http.HandlerFunc(handleCallDownstream)))
	mux.Handle("/api/hello", middleware.TraceID(http.HandlerFunc(handleHello)))
	mux.Handle("/api/async-demo", middleware.TraceID(http.HandlerFunc(handleAsyncDemo)))

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

func handleAsyncDemo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	originalTraceID := trace.FromContext(ctx)

	result := map[string]any{
		"original_trace_id": originalTraceID,
	}

	var brokenTraceID string
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		brokenTraceID = trace.FromContext(context.Background())
	}()
	wg.Wait()
	result["broken_goroutine_trace_id"] = brokenTraceID

	var fixedTraceID string
	var fixedDownstreamTraceID string
	done := async.GoWait(ctx, func(asyncCtx context.Context) {
		fixedTraceID = trace.FromContext(asyncCtx)
		client := httpclient.Default()
		req, err := http.NewRequestWithContext(asyncCtx, http.MethodGet, "http://localhost:9091/downstream/echo", nil)
		if err != nil {
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		fixedDownstreamTraceID = resp.Header.Get(trace.HeaderTraceID)
	})
	<-done
	result["fixed_goroutine_trace_id"] = fixedTraceID
	result["fixed_goroutine_downstream_trace_id"] = fixedDownstreamTraceID

	var parallelIDs []string
	async.GoGroup(ctx,
		func(c context.Context) {
			time.Sleep(5 * time.Millisecond)
			parallelIDs = append(parallelIDs, "task1:"+trace.FromContext(c))
		},
		func(c context.Context) {
			time.Sleep(10 * time.Millisecond)
			parallelIDs = append(parallelIDs, "task2:"+trace.FromContext(c))
		},
		func(c context.Context) {
			time.Sleep(2 * time.Millisecond)
			parallelIDs = append(parallelIDs, "task3:"+trace.FromContext(c))
		},
	)
	result["parallel_goroutine_trace_ids"] = parallelIDs

	wrapped := async.Wrap(ctx, func(c context.Context) {
		result["wrapped_func_trace_id"] = trace.FromContext(c)
	})
	wrapped()

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
