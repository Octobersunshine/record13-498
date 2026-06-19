package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"traceid-demo/async"
	"traceid-demo/httpclient"
	"traceid-demo/logstore"
	"traceid-demo/logx"
	"traceid-demo/middleware"
	"traceid-demo/trace"
)

var globalStore *logstore.MemoryStore

func main() {
	globalStore = logstore.NewMemoryStore()

	upstreamLogger := logx.New("upstream", os.Stdout, globalStore)
	downstreamLogger := logx.New("downstream", os.Stdout, globalStore)

	go startDownstreamServer(":9091", downstreamLogger)

	mux := http.NewServeMux()

	mux.Handle("/api/call-downstream", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleCallDownstream(w, r, upstreamLogger)
	})))
	mux.Handle("/api/hello", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleHello(w, r, upstreamLogger)
	})))
	mux.Handle("/api/async-demo", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAsyncDemo(w, r, upstreamLogger)
	})))
	mux.Handle("/api/logs/", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleLogsQuery(w, r)
	})))

	fmt.Fprintln(os.Stderr, "上游服务启动于 :9090")
	if err := http.ListenAndServe(":9090", mux); err != nil {
		fmt.Fprintf(os.Stderr, "服务启动失败: %v\n", err)
		os.Exit(1)
	}
}

func handleHello(w http.ResponseWriter, r *http.Request, logger *logx.Logger) {
	ctx := r.Context()
	traceID := trace.FromContext(ctx)

	logger.Info(ctx, "收到 hello 请求", map[string]any{
		"method": r.Method,
		"path":   r.URL.Path,
	})
	logger.Debug(ctx, "处理 hello 请求完成", map[string]any{
		"trace_id": traceID,
	})

	fmt.Fprintf(w, "Hello, TraceID: %s\n", traceID)
}

func handleCallDownstream(w http.ResponseWriter, r *http.Request, logger *logx.Logger) {
	ctx := r.Context()
	upstreamTraceID := trace.FromContext(ctx)

	logger.Info(ctx, "开始调用下游服务", map[string]any{
		"downstream_url": "http://localhost:9091/downstream/echo",
	})

	client := httpclient.Default()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:9091/downstream/echo", nil)
	if err != nil {
		logger.Error(ctx, "构造下游请求失败", map[string]any{"error": err.Error()})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Error(ctx, "调用下游服务失败", map[string]any{"error": err.Error()})
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(ctx, "读取下游响应失败", map[string]any{"error": err.Error()})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.Info(ctx, "下游服务调用成功", map[string]any{
		"upstream_trace_id":   upstreamTraceID,
		"downstream_trace_id": resp.Header.Get(trace.HeaderTraceID),
		"response_status":     resp.StatusCode,
	})

	result := map[string]any{
		"upstream_trace_id":   upstreamTraceID,
		"downstream_trace_id": resp.Header.Get(trace.HeaderTraceID),
		"downstream_body":    string(body),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleAsyncDemo(w http.ResponseWriter, r *http.Request, logger *logx.Logger) {
	ctx := r.Context()
	originalTraceID := trace.FromContext(ctx)

	logger.Info(ctx, "开始异步演示", map[string]any{
		"original_trace_id": originalTraceID,
	})

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

	logger.Warn(ctx, "原生 goroutine trace ID 丢失", map[string]any{
		"broken_trace_id": brokenTraceID,
	})

	var fixedTraceID string
	var fixedDownstreamTraceID string
	done := async.GoWait(ctx, func(asyncCtx context.Context) {
		logger.Info(asyncCtx, "异步任务启动，trace ID 已透传", map[string]any{
			"async_trace_id": trace.FromContext(asyncCtx),
		})
		fixedTraceID = trace.FromContext(asyncCtx)
		client := httpclient.Default()
		req, err := http.NewRequestWithContext(asyncCtx, http.MethodGet, "http://localhost:9091/downstream/echo", nil)
		if err != nil {
			logger.Error(asyncCtx, "异步任务构造请求失败", map[string]any{"error": err.Error()})
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			logger.Error(asyncCtx, "异步任务调用下游失败", map[string]any{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		fixedDownstreamTraceID = resp.Header.Get(trace.HeaderTraceID)
		logger.Info(asyncCtx, "异步任务调用下游成功", map[string]any{
			"downstream_trace_id": fixedDownstreamTraceID,
		})
	})
	<-done
	result["fixed_goroutine_trace_id"] = fixedTraceID
	result["fixed_goroutine_downstream_trace_id"] = fixedDownstreamTraceID

	var parallelIDs []string
	async.GoGroup(ctx,
		func(c context.Context) {
			time.Sleep(5 * time.Millisecond)
			logger.Info(c, "并行任务1执行", map[string]any{"task": "task1"})
			parallelIDs = append(parallelIDs, "task1:"+trace.FromContext(c))
		},
		func(c context.Context) {
			time.Sleep(10 * time.Millisecond)
			logger.Info(c, "并行任务2执行", map[string]any{"task": "task2"})
			parallelIDs = append(parallelIDs, "task2:"+trace.FromContext(c))
		},
		func(c context.Context) {
			time.Sleep(2 * time.Millisecond)
			logger.Info(c, "并行任务3执行", map[string]any{"task": "task3"})
			parallelIDs = append(parallelIDs, "task3:"+trace.FromContext(c))
		},
	)
	result["parallel_goroutine_trace_ids"] = parallelIDs

	wrapped := async.Wrap(ctx, func(c context.Context) {
		logger.Info(c, "包装函数执行", map[string]any{"type": "wrapped"})
		result["wrapped_func_trace_id"] = trace.FromContext(c)
	})
	wrapped()

	logger.Info(ctx, "异步演示完成", map[string]any{
		"total_log_entries": len(globalStore.QueryByTraceID(originalTraceID)),
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleLogsQuery(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	prefix := "/api/logs/"

	if path == prefix || path == prefix {
		ids := globalStore.AllTraceIDs()
		stats := globalStore.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"trace_ids": ids,
			"stats":     stats,
		})
		return
	}

	traceID := strings.TrimPrefix(path, prefix)
	if traceID == "" {
		http.Error(w, "missing trace ID", http.StatusBadRequest)
		return
	}

	entries := globalStore.QueryByTraceID(traceID)
	if entries == nil {
		entries = []logx.Entry{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"trace_id": traceID,
		"count":    len(entries),
		"entries":  entries,
	})
}

func startDownstreamServer(addr string, logger *logx.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/downstream/echo", middleware.TraceID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID := trace.FromContext(ctx)
		received := r.Header.Get(trace.HeaderTraceID)

		logger.Info(ctx, "下游服务收到请求", map[string]any{
			"received_trace_id": received,
			"context_trace_id":  traceID,
		})
		logger.Debug(ctx, "下游服务处理中", map[string]any{
			"method": r.Method,
			"path":   r.URL.Path,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"received_trace_id": received,
			"context_trace_id":  traceID,
			"message":           "下游服务处理成功",
		})

		logger.Info(ctx, "下游服务响应完成", map[string]any{
			"trace_id": traceID,
		})
	})))

	fmt.Fprintf(os.Stderr, "下游服务启动于 %s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "下游服务启动失败: %v\n", err)
		os.Exit(1)
	}
}
