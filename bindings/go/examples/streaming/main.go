// Run from the workspace root: go -C bindings/go run ./examples/streaming.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/xconnio/xconn-core/bindings/go"
)

func run() error {
	router, err := xconn.NewRouter(nil)
	if err != nil {
		return err
	}
	defer router.Close()
	if err := router.AddRealm("realm1", nil); err != nil {
		return err
	}
	session, err := xconn.ConnectInMemory(router, "realm1")
	if err != nil {
		return err
	}
	defer session.Close()
	registration := session.Register("example.process", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		if err := inv.SendProgress(inv.Args(), inv.Kwargs()); err != nil {
			return &xconn.Result{Err: err}
		}
		if inv.Progress() {
			return xconn.NewResultError(xconn.ErrNoResult)
		}
		return xconn.NewResult("done")
	}).
		Execute()
	if registration.Err != nil {
		return registration.Err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	call, err := session.Call("example.process").Arg("first").StreamContext(ctx,
		xconn.StreamOptions{ProgressiveInput: true})
	if err != nil {
		return err
	}
	defer call.Close()
	// Two chunks fit in the result buffer; long producers should receive concurrently.
	if err := call.Finish(ctx, []any{"last"}, nil); err != nil {
		return err
	}
	for {
		result, err := call.Receive(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Println(result.Args, result.Progress())
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
