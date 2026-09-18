// Package pubsub provides bounded, process-local comment fan-out.
package pubsub

import (
	"context"
	"sync"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
)

type subscriber struct {
	ch   chan domain.Comment
	done chan struct{}
}

// Broker serializes publish/unsubscribe/close so no sender can race channel
// closure. Overflow disconnects a slow subscriber instead of blocking writers.
type Broker struct {
	mu     sync.Mutex
	byPost map[int64]map[*subscriber]struct{}
	buffer int
	closed bool
}

func New(buffer int) *Broker {
	if buffer < 1 {
		buffer = 64
	}
	return &Broker{byPost: make(map[int64]map[*subscriber]struct{}), buffer: buffer}
}

func (b *Broker) Subscribe(ctx context.Context, postID int64) <-chan domain.Comment {
	s := &subscriber{ch: make(chan domain.Comment, b.buffer), done: make(chan struct{})}
	b.mu.Lock()
	if b.closed || ctx.Err() != nil {
		close(s.ch)
		close(s.done)
		b.mu.Unlock()
		return s.ch
	}
	if b.byPost[postID] == nil {
		b.byPost[postID] = make(map[*subscriber]struct{})
	}
	b.byPost[postID][s] = struct{}{}
	b.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			b.remove(postID, s)
		case <-s.done:
		}
	}()
	return s.ch
}

func (b *Broker) Publish(c domain.Comment) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.byPost[c.PostID] {
		select {
		case s.ch <- c:
		default:
			b.removeLocked(c.PostID, s)
		}
	}
}

func (b *Broker) remove(postID int64, s *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(postID, s)
}

func (b *Broker) removeLocked(postID int64, s *subscriber) {
	if _, ok := b.byPost[postID][s]; !ok {
		return
	}
	delete(b.byPost[postID], s)
	if len(b.byPost[postID]) == 0 {
		delete(b.byPost, postID)
	}
	close(s.ch)
	close(s.done)
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for postID, subscribers := range b.byPost {
		for s := range subscribers {
			b.removeLocked(postID, s)
		}
	}
}
