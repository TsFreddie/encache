package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const downloadResumeDelay = 5 * time.Second

type DownloadGate struct {
	mu             sync.Mutex
	activeStreams  int
	downloading    bool
	lastActiveStop time.Time
	cond           *sync.Cond
}

func NewDownloadGate() *DownloadGate {
	gate := &DownloadGate{}
	gate.cond = sync.NewCond(&gate.mu)
	return gate
}

func (g *DownloadGate) ActiveStarted() func() {
	if g == nil {
		return func() {}
	}
	g.mu.Lock()
	g.activeStreams++
	g.cond.Broadcast()
	active := g.activeStreams
	g.mu.Unlock()
	fmt.Printf("[DownloadGate] active stream opened active=%d\n", active)

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.activeStreams > 0 {
				g.activeStreams--
			}
			g.lastActiveStop = time.Now()
			active := g.activeStreams
			g.cond.Broadcast()
			g.mu.Unlock()
			fmt.Printf("[DownloadGate] active stream closed active=%d\n", active)
		})
	}
}

func (g *DownloadGate) WaitDownloadTurn(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	done := contextDoneBroadcaster(ctx, g.cond)
	defer done()

	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !g.downloading && g.activeStreams == 0 {
			wait := downloadResumeDelay - time.Since(g.lastActiveStop)
			if wait <= 0 {
				g.downloading = true
				return g.releaseDownloadTurn, nil
			}
			if err := g.waitOrContext(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}
		g.cond.Wait()
	}
}

func (g *DownloadGate) WaitDownloadResumed(ctx context.Context) error {
	if g == nil {
		return nil
	}
	done := contextDoneBroadcaster(ctx, g.cond)
	defer done()

	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.activeStreams == 0 {
			wait := downloadResumeDelay - time.Since(g.lastActiveStop)
			if wait <= 0 {
				return nil
			}
			if err := g.waitOrContext(ctx, wait); err != nil {
				return err
			}
			continue
		}
		g.cond.Wait()
	}
}

func (g *DownloadGate) releaseDownloadTurn() {
	g.mu.Lock()
	g.downloading = false
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *DownloadGate) waitOrContext(ctx context.Context, d time.Duration) error {
	timer := time.AfterFunc(d, func() {
		g.cond.L.Lock()
		g.cond.Broadcast()
		g.cond.L.Unlock()
	})
	g.cond.Wait()
	if !timer.Stop() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return ctx.Err()
}

func contextDoneBroadcaster(ctx context.Context, cond *sync.Cond) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		case <-done:
		}
	}()
	return func() { close(done) }
}
