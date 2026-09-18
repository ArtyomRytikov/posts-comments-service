package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
)

func TestDeliveryIsolationCancellationAndClose(t *testing.T) {
	b := New(2)
	ctx, cancel := context.WithCancel(context.Background())
	one := b.Subscribe(ctx, 1)
	two := b.Subscribe(context.Background(), 2)
	b.Publish(domain.Comment{ID: 1, PostID: 1})
	select {
	case c := <-one:
		if c.ID != 1 {
			t.Fatal(c)
		}
	case <-time.After(time.Second):
		t.Fatal("missing event")
	}
	select {
	case <-two:
		t.Fatal("cross-post event")
	default:
	}
	cancel()
	select {
	case _, ok := <-one:
		if ok {
			t.Fatal("channel still open")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel leak")
	}
	b.Close()
	b.Close()
	if _, ok := <-two; ok {
		t.Fatal("close failed")
	}
	if _, ok := <-b.Subscribe(context.Background(), 3); ok {
		t.Fatal("subscribe after close")
	}
	b.Publish(domain.Comment{PostID: 2})
}

func TestSlowSubscriberDisconnected(t *testing.T) {
	b := New(1)
	defer b.Close()
	slow := b.Subscribe(context.Background(), 1)
	fast := b.Subscribe(context.Background(), 1)
	b.Publish(domain.Comment{ID: 1, PostID: 1})
	<-fast
	b.Publish(domain.Comment{ID: 2, PostID: 1})
	if c := <-fast; c.ID != 2 {
		t.Fatal(c)
	}
	if c := <-slow; c.ID != 1 {
		t.Fatal(c)
	}
	if _, ok := <-slow; ok {
		t.Fatal("overflow did not close slow channel")
	}
}

func TestConcurrentSubscribePublishCancelClose(t *testing.T) {
	b := New(4)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			ch := b.Subscribe(ctx, 1)
			b.Publish(domain.Comment{PostID: 1})
			cancel()
			for range ch {
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); b.Close() }()
	wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.byPost) != 0 {
		t.Fatal("retained subscribers")
	}
}
