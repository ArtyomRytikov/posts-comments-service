package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pubsub"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/memory"
	"github.com/gorilla/websocket"
)

func openSocket(t *testing.T, server *httptest.Server, protocol string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{protocol}, HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteJSON(map[string]any{"type": "connection_init"}); err != nil {
		t.Fatal(err)
	}
	var msg struct{ Type string }
	if err := conn.ReadJSON(&msg); err != nil || msg.Type != "connection_ack" {
		t.Fatalf("initialization: %+v, %v", msg, err)
	}
	return conn
}

func subscribeSocket(t *testing.T, conn *websocket.Conn, id, post, kind string) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{"id": id, "type": kind, "payload": map[string]any{"query": fmt.Sprintf(`subscription{commentAdded(postID:%q){id}}`, post)}}); err != nil {
		t.Fatal(err)
	}
}

func requireSocketClose(t *testing.T, conn *websocket.Conn, code int) {
	t.Helper()
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, code) {
		t.Fatalf("want socket close %d, got %v", code, err)
	}
}

func socketFixture(t *testing.T) (*httptest.Server, *watchedEvents, string, int64) {
	t.Helper()
	events := &watchedEvents{Broker: pubsub.New(64), started: make(chan int64, 64), stopped: make(chan int64, 64)}
	t.Cleanup(events.Close)
	server, _ := testServer(t, memory.New(), events)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	postID, _ := strconv.ParseInt(post, 10, 64)
	return server, events, post, postID
}

func TestWebsocketDuplicateOperationCancelsExistingStream(t *testing.T) {
	server, events, post, postID := socketFixture(t)
	conn := openSocket(t, server, "graphql-transport-ws")
	subscribeSocket(t, conn, "same", post, "subscribe")
	waitPost(t, events.started, postID)
	subscribeSocket(t, conn, "same", post, "subscribe")
	requireSocketClose(t, conn, 4409)
	waitPost(t, events.stopped, postID)
	select {
	case <-events.started:
		t.Fatal("duplicate ID created an orphan subscription")
	default:
	}
}

func TestWebsocketOperationLimitCancelsAllStreams(t *testing.T) {
	server, events, post, postID := socketFixture(t)
	conn := openSocket(t, server, "graphql-transport-ws")
	for i := 0; i < 32; i++ {
		subscribeSocket(t, conn, strconv.Itoa(i), post, "subscribe")
		waitPost(t, events.started, postID)
	}
	subscribeSocket(t, conn, "overflow", post, "subscribe")
	requireSocketClose(t, conn, websocket.ClosePolicyViolation)
	for i := 0; i < 32; i++ {
		waitPost(t, events.stopped, postID)
	}
	select {
	case <-events.started:
		t.Fatal("operation over the limit reached the broker")
	default:
	}
}

func TestWebsocketMessageLimit(t *testing.T) {
	server, _, _, _ := socketFixture(t)
	conn := openSocket(t, server, "graphql-transport-ws")
	// The transport caps the entire frame, including variables and init payloads,
	// before GraphQL parsing or operation allocation.
	_ = conn.WriteJSON(map[string]any{"type": "ping", "payload": map[string]any{"padding": strings.Repeat("x", (1<<20)+1)}})
	requireSocketClose(t, conn, websocket.CloseMessageTooBig)
}

func TestWebsocketOperationIDCanBeReusedAfterComplete(t *testing.T) {
	server, events, post, postID := socketFixture(t)
	conn := openSocket(t, server, "graphql-transport-ws")
	for i := 0; i < 20; i++ {
		subscribeSocket(t, conn, "reused", post, "subscribe")
		waitPost(t, events.started, postID)
		if err := conn.WriteJSON(map[string]any{"id": "reused", "type": "complete"}); err != nil {
			t.Fatal(err)
		}
		waitPost(t, events.stopped, postID)
	}
	subscribeSocket(t, conn, "reused", post, "subscribe")
	waitPost(t, events.started, postID)
	wanted := addComment(t, server, post, nil, "after ID reuse")
	var msg struct {
		ID, Type string
		Payload  response
	}
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.ID != "reused" || msg.Type != "next" || nodeID(t, msg.Payload, "commentAdded") != wanted {
		t.Fatalf("wrong event after ID reuse: %+v", msg)
	}
	_ = conn.Close()
	waitPost(t, events.stopped, postID)
}

func TestWebsocketLegacyPlaygroundProtocol(t *testing.T) {
	server, events, post, postID := socketFixture(t)
	conn := openSocket(t, server, "graphql-ws")
	subscribeSocket(t, conn, "legacy", post, "start")
	waitPost(t, events.started, postID)
	wanted := addComment(t, server, post, nil, "legacy event")
	var msg struct {
		ID, Type string
		Payload  response
	}
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "data" || nodeID(t, msg.Payload, "commentAdded") != wanted {
		t.Fatalf("wrong legacy event: %+v", msg)
	}
	if err := conn.WriteJSON(map[string]any{"id": "legacy", "type": "stop"}); err != nil {
		t.Fatal(err)
	}
	waitPost(t, events.stopped, postID)
}

func TestWebsocketServerContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := &watchedEvents{Broker: pubsub.New(32), started: make(chan int64, 2), stopped: make(chan int64, 2)}
	t.Cleanup(events.Close)
	handler := api.New(service.New(memory.New(), events), slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewUnstartedServer(handler)
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	server.Start()
	t.Cleanup(server.Close)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	postID, _ := strconv.ParseInt(post, 10, 64)
	conn := openSocket(t, server, "graphql-transport-ws")
	subscribeSocket(t, conn, "shutdown", post, "subscribe")
	waitPost(t, events.started, postID)
	cancel()
	waitPost(t, events.stopped, postID)
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("socket survived shutdown cancellation")
	}
}

func TestWebsocketInvalidOperationIsSafeAndConnectionRemainsUsable(t *testing.T) {
	server, _, _, _ := socketFixture(t)
	conn := openSocket(t, server, "graphql-transport-ws")
	for _, payload := range []any{nil, "invalid", map[string]any{"query": `{doesNotExist}`}} {
		if err := conn.WriteJSON(map[string]any{"id": "invalid", "type": "subscribe", "payload": payload}); err != nil {
			t.Fatal(err)
		}
		var msg struct {
			Type    string
			Payload []struct {
				Message    string
				Extensions map[string]any
			}
		}
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatal(err)
		}
		if msg.Type != "error" || len(msg.Payload) != 1 || msg.Payload[0].Extensions["code"] != "BAD_USER_INPUT" {
			t.Fatalf("unsafe protocol error: %+v", msg)
		}
	}
	if err := conn.WriteJSON(map[string]any{"id": "valid", "type": "subscribe", "payload": map[string]any{"query": `{posts{edges{node{id}}}}`}}); err != nil {
		t.Fatal(err)
	}
	var msg struct {
		Type    string
		Payload json.RawMessage
	}
	if err := conn.ReadJSON(&msg); err != nil || msg.Type != "next" {
		t.Fatalf("query after malformed input: %+v, %v", msg, err)
	}
	if err := conn.ReadJSON(&msg); err != nil || msg.Type != "complete" {
		t.Fatalf("query completion: %+v, %v", msg, err)
	}
}

func TestWebsocketInitializationDeadlineCannotBeExtendedByPong(t *testing.T) {
	server, _, _, _ := socketFixture(t)
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}, HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(7 * time.Second))
	// A peer cannot replace the five-second initialization deadline with the
	// established connection's heartbeat deadline by sending an early pong.
	if err := conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	requireSocketClose(t, conn, 4408)
}

type deadlineRepo struct {
	repository.Repository
	contexts chan context.Context
}

func (r *deadlineRepo) GetPost(ctx context.Context, id int64) (domain.Post, error) {
	r.contexts <- ctx
	return r.Repository.GetPost(ctx, id)
}

func (r *deadlineRepo) ListCommentPages(ctx context.Context, pages []domain.CommentRequest) ([]domain.CommentPage, error) {
	r.contexts <- ctx
	return r.Repository.ListCommentPages(ctx, pages)
}

func TestOperationDeadlineReachesResolversAndDataLoader(t *testing.T) {
	for _, transport := range []string{"http", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			repo := &deadlineRepo{Repository: memory.New(), contexts: make(chan context.Context, 2)}
			broker := pubsub.New(32)
			t.Cleanup(broker.Close)
			server, _ := testServer(t, repo, broker)
			post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
			query := fmt.Sprintf(`{post(id:%q){id comments{edges{node{id}}}}}`, post)
			if transport == "http" {
				success(t, request(t, server, "", query, nil))
			} else {
				conn := openSocket(t, server, "graphql-transport-ws")
				if err := conn.WriteJSON(map[string]any{"id": "query", "type": "subscribe", "payload": map[string]any{"query": query}}); err != nil {
					t.Fatal(err)
				}
				var msg struct {
					Type    string
					Payload response
				}
				if err := conn.ReadJSON(&msg); err != nil || msg.Type != "next" {
					t.Fatalf("query response: %+v, %v", msg, err)
				}
				success(t, msg.Payload)
				if err := conn.ReadJSON(&msg); err != nil || msg.Type != "complete" {
					t.Fatalf("query completion: %+v, %v", msg, err)
				}
			}
			for i := 0; i < 2; i++ {
				select {
				case ctx := <-repo.contexts:
					deadline, ok := ctx.Deadline()
					remaining := time.Until(deadline)
					if !ok || remaining <= 0 || remaining > 10*time.Second {
						t.Fatalf("repository call lacks operation deadline: %v, %v", deadline, ok)
					}
					select {
					case <-ctx.Done():
					case <-time.After(time.Second):
						t.Fatal("completed operation did not cancel its context")
					}
				case <-time.After(time.Second):
					t.Fatal("query did not invoke the repository and DataLoader")
				}
			}
		})
	}
}

func TestSubscriptionStorageCallsHaveIndependentDeadlines(t *testing.T) {
	repo := &deadlineRepo{Repository: memory.New(), contexts: make(chan context.Context, 8)}
	events := &watchedEvents{Broker: pubsub.New(32), started: make(chan int64, 1), stopped: make(chan int64, 1)}
	t.Cleanup(events.Close)
	server, _ := testServer(t, repo, events)
	post := nodeID(t, request(t, server, "author", createPost, nil), "createPost")
	postID, _ := strconv.ParseInt(post, 10, 64)
	conn := openSocket(t, server, "graphql-transport-ws")
	if err := conn.WriteJSON(map[string]any{"id": "bounded", "type": "subscribe", "payload": map[string]any{
		"query": fmt.Sprintf(`subscription{commentAdded(postID:%q){id replies(first:1){edges{node{id}}}}}`, post),
	}}); err != nil {
		t.Fatal(err)
	}
	waitPost(t, events.started, postID)
	checkRead := func() {
		t.Helper()
		select {
		case ctx := <-repo.contexts:
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Second {
				t.Errorf("subscription storage call has no bounded deadline: %v, %v", deadline, ok)
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Error("completed storage call retained its timeout context")
			}
		case <-time.After(time.Second):
			t.Fatal("missing subscription storage call")
		}
	}
	checkRead() // Initial post lookup.
	for range 2 {
		wanted := addComment(t, server, post, nil, "event with a nested read")
		var msg struct {
			Type    string
			Payload response
		}
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatal(err)
		}
		if msg.Type != "next" || nodeID(t, msg.Payload, "commentAdded") != wanted {
			t.Fatalf("subscription did not survive read-context cancellation: %+v", msg)
		}
		checkRead() // Each event's DataLoader query gets its own deadline.
	}
	_ = conn.Close()
	waitPost(t, events.stopped, postID)
}
