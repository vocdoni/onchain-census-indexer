package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLogIndexerErrorsStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		logIndexerErrors(ctx, errCh)
		close(done)
	}()

	errCh <- errors.New("transient indexer error")
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("logIndexerErrors did not stop after context cancellation")
	}
}
