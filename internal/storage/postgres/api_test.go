package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pubsub"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresGraphQLNestedQueryCount(t *testing.T) {
	store := newTestStore(t, true)
	post := createTestPost(t, store)
	// 40 roots, 160 replies and 160 grandchildren: enough siblings to cross
	// the DataLoader's 100-key batch boundary on the last level.
	for range 40 {
		root := createTestComment(t, store, post.ID, 0)
		for range 4 {
			reply := createTestComment(t, store, post.ID, root.ID)
			createTestComment(t, store, post.ID, reply.ID)
		}
	}
	counter := &queryCounter{}
	cfg := store.pool.Config()
	cfg.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	broker := pubsub.New(64)
	defer broker.Close()
	server := httptest.NewServer(api.New(service.New(&Store{pool: pool}, broker), slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	client := server.Client()
	client.Timeout = 15 * time.Second
	body := []byte(`{"query":"{posts(first:1){edges{node{id comments(first:40){edges{node{id replies(first:4){edges{node{id replies(first:1){edges{node{id}}}}}}}}}}}}}"}`)
	res, err := client.Post(server.URL+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var response struct {
		Data   json.RawMessage
		Errors json.RawMessage
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK || len(response.Errors) != 0 {
		t.Fatalf("GraphQL failed: status=%d errors=%s", res.StatusCode, response.Errors)
	}
	if count := bytes.Count(response.Data, []byte(`"id":`)); count != 361 {
		t.Fatalf("incomplete nested result: got %d nodes, want 361", count)
	}
	// Normally: posts + roots + replies + two batches of grandchildren = 5.
	// Permit scheduler jitter, but never a round trip per parent (202 calls).
	count := counter.queries.Load()
	t.Logf("GraphQL returned 1 post and 360 comments using %d SQL queries", count)
	if count < 5 || count > 10 {
		t.Fatalf("unexpected SQL query count: %d; want 5..10", count)
	}
}

func TestPostgresDeepChainPagedWithoutTraversal(t *testing.T) {
	store := newTestStore(t, true)
	post := createTestPost(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const depth = 1000
	var parent int64
	for range depth {
		comment, err := store.CreateComment(ctx, domain.NewComment{PostID: post.ID, ParentID: parent, AuthorID: "reader", Body: "deep reply"})
		if err != nil {
			t.Fatal(err)
		}
		parent = comment.ID
	}
	var after, previous int64
	count := 0
	for {
		pages, err := store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: post.ID, All: true, Page: domain.Page{After: after, Limit: 100}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, comment := range pages[0].Items {
			if comment.ParentID != previous || comment.ID <= after {
				t.Fatalf("broken chain at %d: %+v", count, comment)
			}
			previous, after = comment.ID, comment.ID
			count++
		}
		if !pages[0].HasNext {
			break
		}
	}
	if count != depth {
		t.Fatalf("read %d comments, want %d", count, depth)
	}
}
