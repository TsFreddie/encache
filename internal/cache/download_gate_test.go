package cache

import (
	"context"
	"testing"
	"time"
)

func TestDownloadGateQueuesOneDownloadAtATime(t *testing.T) {
	gate := NewDownloadGate()
	gate.lastActiveStop = time.Now().Add(-downloadResumeDelay)

	release, err := gate.WaitDownloadTurn(context.Background())
	if err != nil {
		t.Fatalf("first wait: %v", err)
	}

	started := make(chan struct{})
	go func() {
		release, err := gate.WaitDownloadTurn(context.Background())
		if err != nil {
			t.Errorf("second wait: %v", err)
			return
		}
		defer release()
		close(started)
	}()

	select {
	case <-started:
		t.Fatal("second download started while first held queue")
	case <-time.After(25 * time.Millisecond):
	}

	release()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second download did not start after release")
	}
}

func TestDownloadGateWaitsForActiveStreamAndResumeDelay(t *testing.T) {
	gate := NewDownloadGate()
	gate.lastActiveStop = time.Now().Add(-downloadResumeDelay)
	releaseActive := gate.ActiveStarted()

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if release, err := gate.WaitDownloadTurn(ctx); err == nil {
		release()
		t.Fatal("download started while active stream was open")
	}

	releaseActive()
	ctx, cancel = context.WithTimeout(context.Background(), downloadResumeDelay/2)
	defer cancel()
	if release, err := gate.WaitDownloadTurn(ctx); err == nil {
		release()
		t.Fatal("download started before resume delay elapsed")
	}

	gate.mu.Lock()
	gate.lastActiveStop = time.Now().Add(-downloadResumeDelay)
	gate.cond.Broadcast()
	gate.mu.Unlock()
	if release, err := gate.WaitDownloadTurn(context.Background()); err != nil {
		t.Fatalf("download did not start after resume delay: %v", err)
	} else {
		release()
	}
}
