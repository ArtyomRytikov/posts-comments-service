package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pagination"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pubsub"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/memory"
	"github.com/gorilla/websocket"
)

type response struct {
	Data   map[string]json.RawMessage `json:"data"`
	Errors []struct {
		Message    string         `json:"message"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func testServer(t *testing.T, repo repository.Repository, events service.Events) (*httptest.Server, *service.Service) {
	t.Helper()
	svc := service.New(repo, events)
	server := httptest.NewServer(api.New(svc, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(server.Close)
	return server, svc
}

func request(t *testing.T, server *httptest.Server, actor, query string, variables map[string]any) response {
	t.Helper()
	return requestURL(t, server.Client(), server.URL, actor, query, variables)
}

func requestURL(t *testing.T, client *http.Client, baseURL, actor, query string, variables map[string]any) response {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/query", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if actor != "" {
		req.Header.Set("X-User-ID", actor)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var result response
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func success(t *testing.T, result response) {
	t.Helper()
	if len(result.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", result.Errors)
	}
}

func errorCode(t *testing.T, result response, code string) {
	t.Helper()
	if len(result.Errors) != 1 || result.Errors[0].Extensions["code"] != code {
		t.Fatalf("want error %s, got %+v", code, result.Errors)
	}
}

func nodeID(t *testing.T, result response, field string) string {
	t.Helper()
	success(t, result)
	var node struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(result.Data[field], &node); err != nil {
		t.Fatal(err)
	}
	if node.ID == "" {
		t.Fatalf("empty ID in %s", field)
	}
	return node.ID
}

const createPost = `mutation { createPost(input:{title:"Post", body:"Body"}) { id authorID commentsEnabled } }`
const createComment = `mutation($post:ID!, $parent:ID, $body:String!) { createComment(input:{postID:$post, parentID:$parent, body:$body}) { id parentID body } }`

func addComment(t *testing.T, server *httptest.Server, post string, parent any, body string) string {
	t.Helper()
	return nodeID(t, request(t, server, "reader", createComment, map[string]any{"post": post, "parent": parent, "body": body}), "createComment")
}

func TestHTTPWorkflowAndValidation(t *testing.T) {
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, _ := testServer(t, memory.New(), broker)
	errorCode(t, request(t, server, "", createPost, nil), "UNAUTHENTICATED")
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	root := addComment(t, server, post, nil, "root")
	reply := addComment(t, server, post, root, "reply")
	secondRoot := addComment(t, server, post, nil, "second root")
	result := request(t, server, "", `query($id:ID!) { post(id:$id) { comments(first:1) { edges { cursor node { id replies(first:1) { edges { node { id parentID } } } } } pageInfo { hasNextPage endCursor } } commentFeed(first:2) { edges { node { id parentID } } pageInfo { hasNextPage } } } }`, map[string]any{"id": post})
	success(t, result)
	var page struct {
		Comments struct {
			Edges []struct {
				Cursor string
				Node   struct {
					ID      string
					Replies struct {
						Edges []struct {
							Node struct {
								ID       string
								ParentID *string
							}
						}
					}
				}
			}
			PageInfo struct {
				HasNextPage bool
				EndCursor   *string
			}
		}
		CommentFeed struct {
			Edges []struct {
				Node struct {
					ID       string
					ParentID *string
				}
			}
			PageInfo struct{ HasNextPage bool }
		}
	}
	if err := json.Unmarshal(result.Data["post"], &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Comments.Edges) != 1 || page.Comments.Edges[0].Node.ID != root || !page.Comments.PageInfo.HasNextPage {
		t.Fatalf("bad root page: %+v", page)
	}
	replies := page.Comments.Edges[0].Node.Replies.Edges
	if len(replies) != 1 || replies[0].Node.ID != reply || replies[0].Node.ParentID == nil || *replies[0].Node.ParentID != root {
		t.Fatalf("bad replies: %+v", replies)
	}
	if len(page.CommentFeed.Edges) != 2 || page.CommentFeed.Edges[1].Node.ID != reply || !page.CommentFeed.PageInfo.HasNextPage {
		t.Fatalf("bad flat page: %+v", page.CommentFeed)
	}
	result = request(t, server, "", `query($id:ID!, $after:String) { post(id:$id) { comments(first:1, after:$after) { edges { node { id } } pageInfo { hasNextPage } } } }`, map[string]any{"id": post, "after": page.Comments.PageInfo.EndCursor})
	success(t, result)
	if !bytes.Contains(result.Data["post"], []byte(`"id":"`+secondRoot+`"`)) || bytes.Contains(result.Data["post"], []byte(`"hasNextPage":true`)) {
		t.Fatalf("bad second page: %s", result.Data["post"])
	}
	errorCode(t, request(t, server, "", `query($id:ID!, $after:String) { post(id:$id) { commentFeed(after:$after) { edges { node { id } } } } }`, map[string]any{"id": post, "after": page.Comments.PageInfo.EndCursor}), "BAD_USER_INPUT")
	for _, id := range []string{"0", "-1", "01", "+1", "abc", "9223372036854775808"} {
		t.Run("invalid ID "+id, func(t *testing.T) {
			errorCode(t, request(t, server, "", `query($id:ID!){post(id:$id){id}}`, map[string]any{"id": id}), "BAD_USER_INPUT")
		})
	}
	errorCode(t, request(t, server, "", `{post(id:"99999"){id}}`, nil), "NOT_FOUND")
	errorCode(t, request(t, server, "", `{posts(first:0){edges{node{id}}}}`, nil), "BAD_USER_INPUT")
	errorCode(t, request(t, server, "", `{posts(after:"garbage"){edges{node{id}}}}`, nil), "BAD_USER_INPUT")
	other := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	errorCode(t, request(t, server, "reader", createComment, map[string]any{"post": other, "parent": root, "body": "wrong post"}), "BAD_USER_INPUT")
	addComment(t, server, other, nil, strings.Repeat("я", 2000))
	errorCode(t, request(t, server, "reader", createComment, map[string]any{"post": other, "body": strings.Repeat("я", 2001)}), "BAD_USER_INPUT")
	change := `mutation($id:ID!){setCommentsEnabled(postID:$id, enabled:false){id commentsEnabled}}`
	errorCode(t, request(t, server, "reader", change, map[string]any{"id": post}), "FORBIDDEN")
	success(t, request(t, server, "author", change, map[string]any{"id": post}))
	errorCode(t, request(t, server, "reader", createComment, map[string]any{"post": post, "parent": root, "body": "disabled"}), "COMMENTS_DISABLED")
	res, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatal(res.Status)
	}
}

type countingRepo struct {
	repository.Repository
	mu       sync.Mutex
	calls    int
	maxBatch int
}

func (r *countingRepo) ListCommentPages(ctx context.Context, requests []domain.CommentRequest) ([]domain.CommentPage, error) {
	r.mu.Lock()
	r.calls++
	if len(requests) > r.maxBatch {
		r.maxBatch = len(requests)
	}
	r.mu.Unlock()
	return r.Repository.ListCommentPages(ctx, requests)
}

func TestNestedConnectionsBatchStorageAccess(t *testing.T) {
	repo := &countingRepo{Repository: memory.New()}
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, svc := testServer(t, repo, broker)
	for i := 0; i < 16; i++ {
		post, err := svc.CreatePost(context.Background(), "author", "title", "body", true)
		if err != nil {
			t.Fatal(err)
		}
		root, err := svc.CreateComment(context.Background(), "reader", post.ID, 0, "root")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = svc.CreateComment(context.Background(), "reader", post.ID, root.ID, "reply"); err != nil {
			t.Fatal(err)
		}
	}
	result := request(t, server, "", `{posts(first:16){edges{node{comments(first:1){edges{node{replies(first:1){edges{node{id}}}}}}}}}}`, nil)
	success(t, result)
	repo.mu.Lock()
	defer repo.mu.Unlock()
	// There are 32 comment connections. Normally this takes two calls, one
	// per level; allow scheduler jitter without allowing a per-parent query.
	if repo.calls > 4 || repo.maxBatch < 8 {
		t.Fatalf("batching regressed: calls=%d largest batch=%d", repo.calls, repo.maxBatch)
	}
}

func TestSerialMutationsSeeFreshConnections(t *testing.T) {
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, _ := testServer(t, memory.New(), broker)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	result := request(t, server, "author", `mutation($id:ID!){before:setCommentsEnabled(postID:$id,enabled:true){comments{edges{node{id}}}} add:createComment(input:{postID:$id,body:"fresh"}){id} after:setCommentsEnabled(postID:$id,enabled:true){comments{edges{node{id}}}}}`, map[string]any{"id": post})
	success(t, result)
	if !bytes.Contains(result.Data["before"], []byte(`"edges":[]`)) || bytes.Contains(result.Data["after"], []byte(`"edges":[]`)) {
		t.Fatalf("stale mutation data: %+v", result.Data)
	}
}

func TestAliasedConnectionsKeepDistinctPaginationKeys(t *testing.T) {
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, _ := testServer(t, memory.New(), broker)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	root := addComment(t, server, post, nil, "first")
	reply := addComment(t, server, post, root, "reply")
	second := addComment(t, server, post, nil, "second")
	postID, _ := strconv.ParseInt(post, 10, 64)
	rootID, _ := strconv.ParseInt(root, 10, 64)
	after := pagination.Encode(rootID, pagination.CommentScope(postID, 0, false))
	result := request(t, server, "", `query($id:ID!, $after:String!){post(id:$id){one:comments(first:1){edges{node{id}}} two:comments(first:2){edges{node{id}}} later:comments(first:1,after:$after){edges{node{id}}} flat:commentFeed(first:2){edges{node{id}}}}}`, map[string]any{"id": post, "after": after})
	success(t, result)
	var postData map[string]struct {
		Edges []struct{ Node struct{ ID string } }
	}
	if err := json.Unmarshal(result.Data["post"], &postData); err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"one": {root}, "two": {root, second}, "later": {second}, "flat": {root, reply}}
	for alias, ids := range want {
		edges := postData[alias].Edges
		if len(edges) != len(ids) {
			t.Fatalf("%s: want %v, got %+v", alias, ids, edges)
		}
		for i, id := range ids {
			if edges[i].Node.ID != id {
				t.Fatalf("%s: want %v, got %+v", alias, ids, edges)
			}
		}
	}
}

type brokenRepo struct{ repository.Repository }

func (brokenRepo) GetPost(context.Context, int64) (domain.Post, error) {
	return domain.Post{}, errors.New("secret database password")
}

func TestErrorsAreSanitizedAndQueriesBounded(t *testing.T) {
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, _ := testServer(t, brokenRepo{memory.New()}, broker)
	result := request(t, server, "", `{post(id:"1"){id}}`, nil)
	errorCode(t, result, "INTERNAL_SERVER_ERROR")
	if strings.Contains(result.Errors[0].Message, "secret") {
		t.Fatal("leaked internal error")
	}
	errorCode(t, request(t, server, "", `{doesNotExist}`, nil), "BAD_USER_INPUT")
	deep := "id"
	for i := 0; i < 10; i++ {
		deep = "replies(first:1){edges{node{" + deep + "}}}"
	}
	errorCode(t, request(t, server, "", `{post(id:"1"){comments(first:1){edges{node{`+deep+`}}}}}`, nil), "BAD_USER_INPUT")
	errorCode(t, request(t, server, "", `{posts(first:100){edges{node{comments(first:100){edges{node{replies(first:100){edges{node{id body}}}}}}}}}}`, nil), "BAD_USER_INPUT")
	// Fragments and aliases must contribute the same cost as inline fields.
	errorCode(t, request(t, server, "", `query{a:posts(first:100){...PostPage} b:posts(first:100){...PostPage}} fragment PostPage on PostConnection{edges{node{comments(first:100){edges{node{id body}}}}}}`, nil), "BAD_USER_INPUT")
	fragments := `query{post(id:"1"){comments(first:1){edges{node{...D0}}}}}`
	for i := 0; i < 10; i++ {
		fragments += fmt.Sprintf(" fragment D%d on Comment{replies(first:1){edges{node{...D%d}}}}", i, i+1)
	}
	fragments += " fragment D10 on Comment{id}"
	errorCode(t, request(t, server, "", fragments, nil), "BAD_USER_INPUT")
}

type watchedEvents struct {
	*pubsub.Broker
	started  chan int64
	stopped  chan int64
	contexts chan context.Context
}

func (e *watchedEvents) Subscribe(ctx context.Context, postID int64) <-chan domain.Comment {
	stream := e.Broker.Subscribe(ctx, postID)
	if e.contexts != nil {
		e.contexts <- ctx
	}
	e.started <- postID
	go func() { <-ctx.Done(); e.stopped <- postID }()
	return stream
}

func waitPost(t *testing.T, stream <-chan int64, expected int64) {
	t.Helper()
	select {
	case got := <-stream:
		if got != expected {
			t.Fatalf("want post %d, got %d", expected, got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for subscription lifecycle")
	}
}

func TestHTTPSubscriptionsRequireWebsocket(t *testing.T) {
	events := &watchedEvents{Broker: pubsub.New(32), started: make(chan int64, 1), stopped: make(chan int64, 1)}
	t.Cleanup(events.Close)
	server, _ := testServer(t, memory.New(), events)
	server.Client().Timeout = time.Second
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	result := request(t, server, "", `subscription($id:ID!){commentAdded(postID:$id){id}}`, map[string]any{"id": post})
	errorCode(t, result, "BAD_USER_INPUT")
	if result.Errors[0].Message != "subscriptions require a WebSocket connection" {
		t.Fatalf("unexpected transport error: %+v", result.Errors)
	}
	select {
	case <-events.started:
		t.Fatal("HTTP subscription reached the broker")
	default:
	}
}

func TestWebsocketSubscriptionIsolationAndDisconnect(t *testing.T) {
	events := &watchedEvents{Broker: pubsub.New(32), started: make(chan int64, 4), stopped: make(chan int64, 4)}
	t.Cleanup(events.Close)
	server, _ := testServer(t, memory.New(), events)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	other := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}, HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.WriteJSON(map[string]any{"type": "connection_init"}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var message struct {
		Type    string          `json:"type"`
		ID      string          `json:"id"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	if message.Type != "connection_ack" {
		t.Fatalf("expected ack, got %s", message.Type)
	}
	if err := conn.WriteJSON(map[string]any{"id": "watch", "type": "subscribe", "payload": map[string]any{"query": fmt.Sprintf(`subscription{commentAdded(postID:%q){id postID body parentID}}`, post)}}); err != nil {
		t.Fatal(err)
	}
	postID, _ := strconv.ParseInt(post, 10, 64)
	waitPost(t, events.started, postID)
	addComment(t, server, other, nil, "must not be delivered")
	wanted := addComment(t, server, post, nil, "deliver this")
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	if message.Type != "next" || message.ID != "watch" {
		t.Fatalf("unexpected event: %+v", message)
	}
	var event response
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		t.Fatal(err)
	}
	if id := nodeID(t, event, "commentAdded"); id != wanted {
		t.Fatalf("received event for another post: %s", id)
	}
	if err := conn.WriteJSON(map[string]any{"id": "watch", "type": "complete"}); err != nil {
		t.Fatal(err)
	}
	waitPost(t, events.stopped, postID)
	// Reuse the live socket after cancelling just one operation.
	if err := conn.WriteJSON(map[string]any{"id": "again", "type": "subscribe", "payload": map[string]any{"query": fmt.Sprintf(`subscription{commentAdded(postID:%q){id}}`, post)}}); err != nil {
		t.Fatal(err)
	}
	waitPost(t, events.started, postID)
	// A network disconnect must cancel the subscription context and its bridge.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitPost(t, events.stopped, postID)
}

func TestWebsocketRejectsCrossOrigin(t *testing.T) {
	broker := pubsub.New(32)
	t.Cleanup(broker.Close)
	server, _ := testServer(t, memory.New(), broker)
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}, HandshakeTimeout: 3 * time.Second}
	conn, res, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/query", http.Header{"Origin": []string{"https://untrusted.example"}})
	if conn != nil {
		conn.Close()
	}
	if res != nil {
		res.Body.Close()
	}
	if err == nil {
		t.Fatal("cross-origin connection accepted")
	}
}
