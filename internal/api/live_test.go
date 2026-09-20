package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// This test targets an actual server process/container, including startup and
// configuration. Use a disposable server: it creates posts and comments.
func TestRunningServerWorkflow(t *testing.T) {
	base := strings.TrimRight(os.Getenv("TEST_SERVER_URL"), "/")
	if base == "" {
		t.Skip("set TEST_SERVER_URL to test a running disposable server")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		res, err := client.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s: %s", path, res.Status)
		}
	}
	query := func(actor, q string, vars map[string]any) response {
		return requestURL(t, client, base, actor, q, vars)
	}
	post := nodeID(t, query("author", createPost, nil), "createPost")
	add := func(parent any, body string) string {
		return nodeID(t, query("reader", createComment, map[string]any{"post": post, "parent": parent, "body": body}), "createComment")
	}
	root := add(nil, "root")
	reply := add(root, "reply")
	grandchild := add(reply, "grandchild")
	secondRoot := add(nil, "second root")
	var after *string
	want := []string{root, reply, grandchild, secondRoot}
	var got []string
	for {
		res := query("", `query($id:ID!,$after:String){post(id:$id){commentFeed(first:2,after:$after){edges{node{id parentID}} pageInfo{hasNextPage endCursor}}}}`, map[string]any{"id": post, "after": after})
		success(t, res)
		var data struct {
			CommentFeed struct {
				Edges []struct {
					Node struct {
						ID       string
						ParentID *string
					}
				}
				PageInfo struct {
					HasNextPage bool
					EndCursor   *string
				}
			}
		}
		if err := json.Unmarshal(res.Data["post"], &data); err != nil {
			t.Fatal(err)
		}
		for _, edge := range data.CommentFeed.Edges {
			got = append(got, edge.Node.ID)
			if edge.Node.ID == grandchild && (edge.Node.ParentID == nil || *edge.Node.ParentID != reply) {
				t.Fatal("grandchild lost its parent")
			}
		}
		if !data.CommentFeed.PageInfo.HasNextPage {
			break
		}
		next := data.CommentFeed.PageInfo.EndCursor
		if next == nil || *next == "" || len(data.CommentFeed.Edges) == 0 ||
			(after != nil && *next == *after) || len(got) >= len(want) {
			t.Fatal("pagination failed to advance")
		}
		after = next
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pagination: got %v, want %v", got, want)
	}
	errorCode(t, query("other", `mutation($id:ID!){setCommentsEnabled(postID:$id,enabled:false){id}}`, map[string]any{"id": post}), "FORBIDDEN")
	success(t, query("author", `mutation($id:ID!){setCommentsEnabled(postID:$id,enabled:false){id}}`, map[string]any{"id": post}))
	errorCode(t, query("reader", createComment, map[string]any{"post": post, "body": "disabled"}), "COMMENTS_DISABLED")
	success(t, query("author", `mutation($id:ID!){setCommentsEnabled(postID:$id,enabled:true){id}}`, map[string]any{"id": post}))

	// There is no per-subscription acknowledgement in graphql-transport-ws.
	// Publish bounded probes until one is observed, instead of sleeping and
	// assuming registration has completed.
	conn := openSocket(t, &httptest.Server{URL: base}, "graphql-transport-ws")
	subscribeSocket(t, conn, "live", post, "subscribe")
	type event struct {
		Type    string
		Payload response
	}
	messages := make(chan event, 1)
	readErrors := make(chan error, 1)
	go func() {
		var msg event
		if err := conn.ReadJSON(&msg); err != nil {
			readErrors <- err
			return
		}
		messages <- msg
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	published := make(map[string]bool)
	for {
		select {
		case <-ticker.C:
			published[add(root, "subscription probe")] = true
		case msg := <-messages:
			if msg.Type != "next" || !published[nodeID(t, msg.Payload, "commentAdded")] {
				t.Fatalf("unexpected subscription event: %+v", msg)
			}
			t.Log("running server: health, create, nested replies, pagination, permissions and WebSocket delivery passed")
			return
		case err := <-readErrors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("running server did not deliver a subscription event")
		}
	}
}
