package memory_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/memory"
)

func newPost(t *testing.T, store *memory.Store) domain.Post {
	t.Helper()
	post, err := store.CreatePost(context.Background(), domain.NewPost{
		AuthorID: "author", Title: "Title", Body: "Body", CommentsEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return post
}

func newComment(t *testing.T, store *memory.Store, postID, parentID int64) domain.Comment {
	t.Helper()
	comment, err := store.CreateComment(context.Background(), domain.NewComment{
		PostID: postID, ParentID: parentID, AuthorID: "reader", Body: "Comment",
	})
	if err != nil {
		t.Fatal(err)
	}
	return comment
}

func TestPostPermissionsAndIndependentValues(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	post := newPost(t, store)
	if post.ID < 1 || post.CreatedAt.IsZero() {
		t.Fatalf("missing server-generated fields: %+v", post)
	}
	post.Title = "changed by caller"
	stored, err := store.GetPost(ctx, post.ID)
	if err != nil || stored.Title != "Title" {
		t.Fatalf("GetPost = %+v, %v", stored, err)
	}
	if _, err = store.SetCommentsEnabled(ctx, post.ID, "someone else", false); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("unauthorized update: %v", err)
	}
	newComment(t, store, post.ID, 0) // Rejected authorization must not mutate the post.
	if _, err = store.SetCommentsEnabled(ctx, post.ID, "author", false); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateComment(ctx, domain.NewComment{PostID: post.ID}); !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatalf("comment on disabled post: %v", err)
	}
	page, err := store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: post.ID, All: true, Page: domain.Page{Limit: 10}}})
	if err != nil || len(page[0].Items) != 1 {
		t.Fatalf("disabling comments changed existing comments: %+v, %v", page, err)
	}
	page[0].Items[0].Body = "changed by caller"
	page, err = store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: post.ID, All: true, Page: domain.Page{Limit: 10}}})
	if err != nil || page[0].Items[0].Body != "Comment" {
		t.Fatalf("comment page leaked mutable storage: %+v, %v", page, err)
	}
	if _, err = store.SetCommentsEnabled(ctx, post.ID, "author", true); err != nil {
		t.Fatal(err)
	}
	newComment(t, store, post.ID, 0)
	if _, err = store.GetPost(ctx, post.ID+1); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing post: %v", err)
	}
	if _, err = store.SetCommentsEnabled(ctx, post.ID+1, "author", true); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("update missing post: %v", err)
	}
}

func TestPostKeysetPages(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	for range 5 {
		newPost(t, store)
	}
	for _, test := range []struct {
		after   int64
		limit   int
		wantIDs []int64
		hasNext bool
	}{
		{0, 2, []int64{1, 2}, true},
		{2, 2, []int64{3, 4}, true},
		{3, 2, []int64{4, 5}, false},
		{4, 2, []int64{5}, false},
		{5, 2, nil, false},
		{999, 2, nil, false},
	} {
		t.Run(fmt.Sprintf("after_%d", test.after), func(t *testing.T) {
			page, err := store.ListPosts(ctx, domain.Page{After: test.after, Limit: test.limit})
			if err != nil {
				t.Fatal(err)
			}
			var ids []int64
			for _, post := range page.Items {
				ids = append(ids, post.ID)
			}
			if !slices.Equal(ids, test.wantIDs) || page.HasNext != test.hasNext {
				t.Fatalf("page = %v, HasNext %t; want %v, %t", ids, page.HasNext, test.wantIDs, test.hasNext)
			}
		})
	}
	page, err := store.ListPosts(ctx, domain.Page{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	page.Items[0].Title = "mutated"
	post, err := store.GetPost(ctx, 1)
	if err != nil || post.Title != "Title" {
		t.Fatalf("post page leaked mutable storage: %+v, %v", post, err)
	}
	newPost(t, store)
	page, err = store.ListPosts(ctx, domain.Page{After: 4, Limit: 2})
	if err != nil || len(page.Items) != 2 || page.Items[0].ID != 5 || page.Items[1].ID != 6 || page.HasNext {
		t.Fatalf("insert between page reads: %+v, %v", page, err)
	}
}

func TestCommentPagesAndParentIsolation(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	firstPost, secondPost := newPost(t, store), newPost(t, store)
	root := newComment(t, store, firstPost.ID, 0)
	otherRoot := newComment(t, store, secondPost.ID, 0)
	firstChild := newComment(t, store, firstPost.ID, root.ID)
	secondRoot := newComment(t, store, firstPost.ID, 0)
	secondChild := newComment(t, store, firstPost.ID, root.ID)
	grandchild := newComment(t, store, firstPost.ID, firstChild.ID)
	requests := []domain.CommentRequest{
		{PostID: firstPost.ID, All: true, Page: domain.Page{Limit: 3}},
		{PostID: firstPost.ID, All: true, Page: domain.Page{After: secondRoot.ID, Limit: 3}},
		{PostID: firstPost.ID, Page: domain.Page{Limit: 2}},
		{PostID: firstPost.ID, ParentID: root.ID, Page: domain.Page{Limit: 1}},
		{PostID: firstPost.ID, ParentID: root.ID, Page: domain.Page{After: firstChild.ID, Limit: 1}},
		{PostID: firstPost.ID, ParentID: firstChild.ID, Page: domain.Page{Limit: 1}},
		{PostID: secondPost.ID, Page: domain.Page{Limit: 10}},
		{PostID: secondPost.ID, ParentID: root.ID, Page: domain.Page{Limit: 10}},
	}
	want := [][]int64{
		{root.ID, firstChild.ID, secondRoot.ID}, {secondChild.ID, grandchild.ID},
		{root.ID, secondRoot.ID}, {firstChild.ID}, {secondChild.ID}, {grandchild.ID}, {otherRoot.ID}, {},
	}
	pages, err := store.ListCommentPages(ctx, requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != len(requests) {
		t.Fatalf("got %d pages for %d requests", len(pages), len(requests))
	}
	for i, page := range pages {
		var ids []int64
		for _, comment := range page.Items {
			ids = append(ids, comment.ID)
		}
		if !slices.Equal(ids, want[i]) || page.HasNext != (i == 0 || i == 3) {
			t.Fatalf("page %d = %+v, want IDs %v", i, page, want[i])
		}
	}
	for _, input := range []domain.NewComment{
		{PostID: firstPost.ID, ParentID: otherRoot.ID},
		{PostID: firstPost.ID, ParentID: 9999},
		{PostID: firstPost.ID, ParentID: -1},
	} {
		if _, err := store.CreateComment(ctx, input); !errors.Is(err, domain.ErrParentNotFound) {
			t.Fatalf("invalid parent %+v: %v", input, err)
		}
	}
	if _, err := store.CreateComment(ctx, domain.NewComment{PostID: 9999}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing post: %v", err)
	}
}

func TestDeepChainWithoutRecursiveTraversal(t *testing.T) {
	store := memory.New()
	post := newPost(t, store)
	const depth = 10000
	requests := make([]domain.CommentRequest, 0, depth+1)
	var parent int64
	for range depth {
		requests = append(requests, domain.CommentRequest{PostID: post.ID, ParentID: parent, Page: domain.Page{Limit: 1}})
		parent = newComment(t, store, post.ID, parent).ID
	}
	requests = append(requests, domain.CommentRequest{PostID: post.ID, ParentID: parent, Page: domain.Page{Limit: 1}})
	pages, err := store.ListCommentPages(context.Background(), requests)
	if err != nil {
		t.Fatal(err)
	}
	for i := range depth {
		if len(pages[i].Items) != 1 || pages[i].Items[0].ParentID != requests[i].ParentID || pages[i].HasNext {
			t.Fatalf("chain broken at depth %d: %+v", i, pages[i])
		}
	}
	if len(pages[depth].Items) != 0 || pages[depth].HasNext {
		t.Fatalf("leaf has unexpected children: %+v", pages[depth])
	}
}

func TestConcurrentWritesAndPagination(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	post := newPost(t, store)
	const workers, perWorker = 16, 64
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for range perWorker {
				if _, err := store.CreateComment(ctx, domain.NewComment{PostID: post.ID, Body: "Concurrent"}); err != nil {
					t.Error(err)
					return
				}
				if _, err := store.CreatePost(ctx, domain.NewPost{AuthorID: "author"}); err != nil {
					t.Error(err)
					return
				}
				if _, err := store.ListPosts(ctx, domain.Page{Limit: 10}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	group.Wait()
	var after int64
	count := 0
	for {
		pages, err := store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: post.ID, All: true, Page: domain.Page{After: after, Limit: 37}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, comment := range pages[0].Items {
			if comment.ID != after+1 {
				t.Fatalf("noncontiguous or duplicate IDs: %d after %d", comment.ID, after)
			}
			after = comment.ID
			count++
		}
		if !pages[0].HasNext {
			break
		}
	}
	if count != workers*perWorker {
		t.Fatalf("got %d comments, want %d", count, workers*perWorker)
	}
	last, err := store.GetPost(ctx, workers*perWorker+1)
	if err != nil || last.ID != workers*perWorker+1 {
		t.Fatalf("concurrent posts lost: %+v, %v", last, err)
	}
}

func TestConcurrentPermissionChanges(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	post := newPost(t, store)
	var group sync.WaitGroup
	group.Go(func() {
		for i := range 200 {
			if _, err := store.SetCommentsEnabled(ctx, post.ID, "author", i%2 == 0); err != nil {
				t.Error(err)
			}
		}
	})
	for range 8 {
		group.Go(func() {
			for range 200 {
				_, err := store.CreateComment(ctx, domain.NewComment{PostID: post.ID})
				if err != nil && !errors.Is(err, domain.ErrCommentsDisabled) {
					t.Error(err)
				}
			}
		})
	}
	group.Wait()
	if _, err := store.CreateComment(ctx, domain.NewComment{PostID: post.ID}); !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatalf("final disable did not prevent subsequent writes: %v", err)
	}
}

func TestInvalidPages(t *testing.T) {
	store := memory.New()
	for _, test := range []struct {
		page domain.Page
		err  error
	}{
		{domain.Page{Limit: 0}, domain.ErrInvalidPage},
		{domain.Page{Limit: -1}, domain.ErrInvalidPage},
		{domain.Page{Limit: 101}, domain.ErrInvalidPage},
		{domain.Page{After: -1, Limit: 1}, domain.ErrInvalidCursor},
	} {
		if _, err := store.ListPosts(context.Background(), test.page); !errors.Is(err, test.err) {
			t.Errorf("ListPosts(%+v): %v, want %v", test.page, err, test.err)
		}
		if _, err := store.ListCommentPages(context.Background(), []domain.CommentRequest{{Page: test.page}}); !errors.Is(err, test.err) {
			t.Errorf("ListCommentPages(%+v): %v, want %v", test.page, err, test.err)
		}
	}
}

func TestCanceledContext(t *testing.T) {
	store := memory.New()
	post := newPost(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := map[string]func() error{
		"CreatePost":         func() error { _, err := store.CreatePost(ctx, domain.NewPost{}); return err },
		"GetPost":            func() error { _, err := store.GetPost(ctx, post.ID); return err },
		"ListPosts":          func() error { _, err := store.ListPosts(ctx, domain.Page{Limit: 1}); return err },
		"SetCommentsEnabled": func() error { _, err := store.SetCommentsEnabled(ctx, post.ID, "author", false); return err },
		"CreateComment":      func() error { _, err := store.CreateComment(ctx, domain.NewComment{PostID: post.ID}); return err },
		"ListCommentPages":   func() error { _, err := store.ListCommentPages(ctx, nil); return err },
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := test(); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want context.Canceled", err)
			}
		})
	}
	stored, err := store.GetPost(context.Background(), post.ID)
	if err != nil || !stored.CommentsEnabled {
		t.Fatalf("canceled write mutated storage: %+v, %v", stored, err)
	}
	page, err := store.ListPosts(context.Background(), domain.Page{Limit: 10})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("canceled post creation mutated storage: %+v, %v", page, err)
	}
}
