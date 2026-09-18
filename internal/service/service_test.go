package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pubsub"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/memory"
)

func newService(t *testing.T) *service.Service {
	t.Helper()
	b := pubsub.New(64)
	t.Cleanup(b.Close)
	return service.New(memory.New(), b)
}

func TestPostAndCommentWorkflow(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	p, err := s.CreatePost(ctx, "alice", "Hello", "Content", true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPost(ctx, p.ID)
	if err != nil || got != p {
		t.Fatalf("get: %+v %v", got, err)
	}
	posts, err := s.Posts(ctx, domain.Page{Limit: 10})
	if err != nil || len(posts.Items) != 1 || posts.HasNext {
		t.Fatalf("list: %+v %v", posts, err)
	}
	ch, err := s.Subscribe(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.CreateComment(ctx, "bob", p.ID, 0, "Root")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-ch:
		if event != c {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing comment notification")
	}
	child, err := s.CreateComment(ctx, "alice", p.ID, c.ID, "Reply")
	if err != nil || child.ParentID != c.ID {
		t.Fatalf("reply: %+v %v", child, err)
	}
	pages, err := s.CommentPages(ctx, []domain.CommentRequest{{PostID: p.ID, Page: domain.Page{Limit: 1}}, {PostID: p.ID, ParentID: c.ID, Page: domain.Page{Limit: 10}}, {PostID: p.ID, All: true, Page: domain.Page{Limit: 1}}})
	if err != nil || len(pages) != 3 {
		t.Fatalf("pages: %+v %v", pages, err)
	}
	if pages[0].Items[0] != c || pages[0].HasNext || pages[1].Items[0] != child || !pages[2].HasNext {
		t.Fatal(pages)
	}
	if _, err := s.SetCommentsEnabled(ctx, p.ID, "bob", false); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal(err)
	}
	if _, err := s.SetCommentsEnabled(ctx, p.ID, "alice", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, "bob", p.ID, c.ID, "Denied"); !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatal(err)
	}
	if _, err := s.SetCommentsEnabled(ctx, p.ID, "alice", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, "bob", p.ID, 0, "Allowed"); err != nil {
		t.Fatal(err)
	}
}

func TestValidationAndUnicodeBoundaries(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	p, _ := s.CreatePost(ctx, "alice", "Title", "Body", true)
	for _, tt := range []struct {
		name, actor, body string
		post, parent      int64
		want              error
	}{
		{"missing identity", "", "hi", p.ID, 0, domain.ErrUnauthenticated},
		{"invalid identity", " alice", "hi", p.ID, 0, domain.ErrInvalidInput},
		{"unknown post", "a", "hi", 1000, 0, domain.ErrNotFound},
		{"negative post", "a", "hi", -1, 0, domain.ErrInvalidInput},
		{"negative parent", "a", "hi", p.ID, -1, domain.ErrInvalidInput},
		{"unknown parent", "a", "hi", p.ID, 999, domain.ErrParentNotFound},
		{"empty", "a", " \n\t", p.ID, 0, domain.ErrInvalidInput},
		{"unicode too long", "a", strings.Repeat("я", 2001), p.ID, 0, domain.ErrCommentTooLong},
		{"invalid utf8", "a", "\xff", p.ID, 0, domain.ErrInvalidInput},
		{"nul", "a", "hello\x00", p.ID, 0, domain.ErrInvalidInput},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.CreateComment(ctx, tt.actor, tt.post, tt.parent, tt.body)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
	if _, err := s.CreateComment(ctx, "a", p.ID, 0, strings.Repeat("🙂", 2000)); err != nil {
		t.Fatal(err)
	}
	for _, title := range []string{"", " ", strings.Repeat("x", 201)} {
		if _, err := s.CreatePost(ctx, "a", title, "body", true); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if _, err := s.CreatePost(ctx, "", "title", "body", true); !errors.Is(err, domain.ErrUnauthenticated) {
		t.Fatal(err)
	}
	if _, err := s.GetPost(ctx, 0); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := s.Subscribe(ctx, 999); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestNoEventForRejectedComment(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	p, _ := s.CreatePost(ctx, "a", "title", "body", false)
	ch, err := s.Subscribe(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, "a", p.ID, 0, "denied"); !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("event for rejected write")
	default:
	}
}

func TestInvalidPages(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	for _, p := range []domain.Page{{Limit: 0}, {Limit: 101}, {Limit: 10, After: -1}} {
		if _, err := s.Posts(ctx, p); err == nil {
			t.Fatal("invalid posts page accepted")
		}
		if _, err := s.CommentPages(ctx, []domain.CommentRequest{{PostID: 1, Page: p}}); err == nil {
			t.Fatal("invalid comment page accepted")
		}
	}
}
