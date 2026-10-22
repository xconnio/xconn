package xconn

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/xconnio/xconn-core/bindings/go/internal/native"
)

func nativeUint64(value any) uint64 {
	switch v := value.(type) {
	case uint64:
		return v
	case int64:
		if v < 0 {
			panic("negative native identifier")
		}

		return uint64(v)
	default:
		panic(fmt.Sprintf("invalid native integer %T", value))
	}
}

func nativeFields(value any) map[string]any {
	if value == nil {
		return nil
	}

	return value.(map[string]any)
}

func decodeResult(value any) *Result {
	f := nativeFields(value)
	args, _ := f["args"].([]any)
	return &Result{
		invocationID: optionalNativeUint(f["id"]),
		Args:         args,
		Kwargs:       nativeFields(f["kwargs"]),
		Details:      nativeFields(f["details"]),
	}
}

func checkResponse(value any) (any, error) {
	if fields, ok := value.(map[string]any); ok && fields["error"] != nil {
		e := nativeFields(fields["error"])
		args, _ := e["args"].([]any)
		return nil, &Error{URI: e["uri"].(string), Args: args, Kwargs: nativeFields(e["kwargs"])}
	}

	return value, nil
}

type completionDispatcher struct {
	once    sync.Once
	mu      sync.Mutex
	pending map[uint64]chan struct{}
}

//nolint:gochecknoglobals // Exactly one consumer owns the process-wide native completion stream.
var defaultDispatcher = &completionDispatcher{pending: make(map[uint64]chan struct{})}

func (d *completionDispatcher) run() {
	for {
		value, err := native.Call(native.NextCompletion, 0, nil)
		if err != nil {
			panic(err)
		}

		id := nativeUint64(value)
		d.mu.Lock()
		ready := d.pending[id]
		delete(d.pending, id)
		d.mu.Unlock()

		if ready != nil {
			close(ready)
		}
	}
}

func invoke(ctx context.Context, method string, fields map[string]any) (any, error) {
	return defaultDispatcher.invoke(ctx, method, fields)
}

func (d *completionDispatcher) invoke(ctx context.Context, method string, fields map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if fields == nil {
		fields = map[string]any{}
	}

	fields["method"] = method
	d.once.Do(func() { go d.run() })
	// Register under the lock so an immediate native completion cannot overtake
	// its waiter. The dispatcher uses one native wait for every session/call.
	d.mu.Lock()
	started, err := native.Call(native.Submit, 0, fields)
	if err != nil {
		d.mu.Unlock()
		return nil, err
	}

	job := nativeUint64(started)
	ready := make(chan struct{})
	d.pending[job] = ready
	d.mu.Unlock()
	cancelJob := func() {
		d.mu.Lock()
		delete(d.pending, job)
		d.mu.Unlock()
		_, _ = native.Call(native.Cancel, job, nil)
	}

	select {
	case <-ctx.Done():
		cancelJob()
		return nil, ctx.Err()
	case <-ready:
		if err := ctx.Err(); err != nil {
			cancelJob()
			return nil, err
		}

		value, err := native.Call(native.Take, job, nil)
		if err != nil {
			return nil, err
		}

		return checkResponse(value)
	}
}

func nativeInt64(value any) int64 {
	unsigned := nativeUint64(value)
	if unsigned > math.MaxInt64 {
		panic("native identifier exceeds int64")
	}

	return int64(unsigned)
}

func closeHandle(handle uint64) {
	_, _ = native.Call(native.Execute, 0, map[string]any{"method": "close", "handle": handle})
}

func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

func optionalNativeUint(value any) uint64 {
	if value == nil {
		return 0
	}

	return nativeUint64(value)
}
