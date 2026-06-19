package httpclient

import (
	"context"
	"net/http"

	"traceid-demo/trace"
)

type Transport struct {
	Base http.RoundTripper
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	newReq := req.Clone(req.Context())
	if traceID := trace.FromContext(req.Context()); traceID != "" {
		newReq.Header.Set(trace.HeaderTraceID, traceID)
	}
	return base.RoundTrip(newReq)
}

func New(base *http.Client) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	transport := base.Transport
	return &http.Client{
		Transport:     &Transport{Base: transport},
		CheckRedirect: base.CheckRedirect,
		Jar:           base.Jar,
		Timeout:       base.Timeout,
	}
}

func Default() *http.Client {
	return New(nil)
}

func NewRequestWithContext(ctx context.Context, method, url string, body any) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	if traceID := trace.FromContext(ctx); traceID != "" {
		req.Header.Set(trace.HeaderTraceID, traceID)
	}
	return req, nil
}
