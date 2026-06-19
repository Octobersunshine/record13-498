package async

import (
	"context"
	"sync"

	"traceid-demo/trace"
)

func Go(ctx context.Context, fn func(ctx context.Context)) {
	snap := trace.SnapshotFromContext(ctx)
	go func() {
		asyncCtx := snap.Apply(context.Background())
		fn(asyncCtx)
	}()
}

func GoWait(ctx context.Context, fn func(ctx context.Context)) <-chan struct{} {
	snap := trace.SnapshotFromContext(ctx)
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		asyncCtx := snap.Apply(context.Background())
		fn(asyncCtx)
	}()
	return done
}

func GoErr(ctx context.Context, fn func(ctx context.Context) error) <-chan error {
	snap := trace.SnapshotFromContext(ctx)
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		asyncCtx := snap.Apply(context.Background())
		errCh <- fn(asyncCtx)
	}()
	return errCh
}

func GoGroup(ctx context.Context, fns ...func(ctx context.Context)) {
	snap := trace.SnapshotFromContext(ctx)
	var wg sync.WaitGroup
	for _, fn := range fns {
		wg.Add(1)
		go func(f func(ctx context.Context)) {
			defer wg.Done()
			asyncCtx := snap.Apply(context.Background())
			f(asyncCtx)
		}(fn)
	}
	wg.Wait()
}

func Wrap(ctx context.Context, fn func(ctx context.Context)) func() {
	snap := trace.SnapshotFromContext(ctx)
	return func() {
		asyncCtx := snap.Apply(context.Background())
		fn(asyncCtx)
	}
}

func WrapErr(ctx context.Context, fn func(ctx context.Context) error) func() error {
	snap := trace.SnapshotFromContext(ctx)
	return func() error {
		asyncCtx := snap.Apply(context.Background())
		return fn(asyncCtx)
	}
}
