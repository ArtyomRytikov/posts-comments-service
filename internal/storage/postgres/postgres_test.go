package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every test owns a randomly named schema. TEST_DATABASE_URL must identify a
// database in which the test role may create schemas; existing data is untouched.
func newTestStore(t *testing.T, migrate bool) *Store {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	schema := "posts_comments_test_" + hex.EncodeToString(suffix[:])
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close(ctx)
		t.Fatalf("create isolated schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("clean integration schema: %v", err)
		}
		admin.Close(cleanupCtx)
	})
	if strings.HasPrefix(databaseURL, "postgres://") || strings.HasPrefix(databaseURL, "postgresql://") {
		u, err := url.Parse(databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		databaseURL = u.String()
	} else {
		databaseURL += " search_path=" + schema
	}
	store, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if migrate {
		if err := store.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func createTestPost(t *testing.T, store *Store) domain.Post {
	t.Helper()
	p, err := store.CreatePost(context.Background(), domain.NewPost{
		AuthorID: "author", Title: "Test post", Body: "Body", CommentsEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func createTestComment(t *testing.T, store *Store, postID, parentID int64) domain.Comment {
	t.Helper()
	c, err := store.CreateComment(context.Background(), domain.NewComment{
		PostID: postID, ParentID: parentID, AuthorID: "reader", Body: "Привет 👋",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPostgresMigrationsConcurrentAndIdempotent(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	errs := make(chan error, 6)
	for range 6 {
		go func() { errs <- store.Migrate(ctx) }()
	}
	for range 6 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM posts_comments_schema_migrations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("migration count = %d, error = %v", count, err)
	}
	createTestPost(t, store)
}

func TestPostgresPostsAndPermissions(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	ctx := context.Background()
	p1, p2 := createTestPost(t, store), createTestPost(t, store)
	got, err := store.GetPost(ctx, p1.ID)
	if err != nil || !reflect.DeepEqual(got, p1) {
		t.Fatalf("GetPost = %#v, %v; want %#v", got, err, p1)
	}
	page, err := store.ListPosts(ctx, domain.Page{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != p1.ID || !page.HasNext {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	page, err = store.ListPosts(ctx, domain.Page{After: p1.ID, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != p2.ID || page.HasNext {
		t.Fatalf("last page = %#v, %v", page, err)
	}
	if _, err := store.GetPost(ctx, 999999); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing post = %v", err)
	}
	if _, err := store.SetCommentsEnabled(ctx, p1.ID, "other", false); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("unauthorized toggle = %v", err)
	}
	if _, err := store.SetCommentsEnabled(ctx, 999999, p1.AuthorID, false); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing post toggle = %v", err)
	}
	updated, err := store.SetCommentsEnabled(ctx, p1.ID, p1.AuthorID, false)
	if err != nil || updated.CommentsEnabled {
		t.Fatalf("disable comments = %#v, %v", updated, err)
	}
	if _, err := store.CreateComment(ctx, domain.NewComment{PostID: p1.ID, AuthorID: "reader", Body: "hello"}); !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatalf("disabled comment = %v", err)
	}
	if _, err := store.SetCommentsEnabled(ctx, p1.ID, p1.AuthorID, true); err != nil {
		t.Fatal(err)
	}
	createTestComment(t, store, p1.ID, 0)
}

func TestPostgresCommentIntegrityAndUnicode(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	ctx := context.Background()
	p1, p2 := createTestPost(t, store), createTestPost(t, store)
	parent := createTestComment(t, store, p1.ID, 0)
	for _, input := range []domain.NewComment{
		{PostID: p2.ID, ParentID: parent.ID, AuthorID: "reader", Body: "wrong post"},
		{PostID: p1.ID, ParentID: 999999, AuthorID: "reader", Body: "missing parent"},
		// Even an ID matching the next sequence value must be a missing parent.
		{PostID: p1.ID, ParentID: parent.ID + 1, AuthorID: "reader", Body: "self parent"},
	} {
		if _, err := store.CreateComment(ctx, input); !errors.Is(err, domain.ErrParentNotFound) {
			t.Fatalf("invalid parent %d/%d = %v", input.PostID, input.ParentID, err)
		}
	}
	if _, err := store.CreateComment(ctx, domain.NewComment{PostID: 999999, AuthorID: "reader", Body: "missing"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing owning post = %v", err)
	}
	input := domain.NewComment{PostID: p1.ID, AuthorID: "reader", Body: strings.Repeat("🙂", domain.MaxCommentLength)}
	if _, err := store.CreateComment(ctx, input); err != nil {
		t.Fatalf("2000 Unicode characters rejected: %v", err)
	}
	input.Body += "🙂"
	if _, err := store.CreateComment(ctx, input); !errors.Is(err, domain.ErrCommentTooLong) {
		t.Fatalf("2001 Unicode characters = %v", err)
	}
	// Verify invariants are also enforced below the repository boundary.
	_, err := store.pool.Exec(ctx, `INSERT INTO comments (post_id, parent_id, author_id, body) VALUES ($1, $2, 'reader', 'invalid')`, p2.ID, parent.ID)
	if !errors.Is(mapError(err), domain.ErrParentNotFound) {
		t.Fatalf("database accepted cross-post parent: %v", err)
	}
	_, err = store.pool.Exec(ctx, `INSERT INTO comments (post_id, author_id, body) VALUES ($1, 'reader', $2)`, p1.ID, input.Body)
	if !errors.Is(mapError(err), domain.ErrCommentTooLong) {
		t.Fatalf("database accepted oversized body: %v", err)
	}
}

type queryCounter struct{ queries atomic.Int64 }

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.queries.Add(1)
	return ctx
}
func (*queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestPostgresBatchPagesUseOneQuery(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	p1, p2 := createTestPost(t, store), createTestPost(t, store)
	root1 := createTestComment(t, store, p1.ID, 0)
	child1 := createTestComment(t, store, p1.ID, root1.ID)
	grandchild := createTestComment(t, store, p1.ID, child1.ID)
	root2 := createTestComment(t, store, p1.ID, 0)
	child2 := createTestComment(t, store, p1.ID, root1.ID)
	other := createTestComment(t, store, p2.ID, 0)
	requests := []domain.CommentRequest{
		{PostID: p1.ID, Page: domain.Page{Limit: 1}},
		{PostID: p1.ID, ParentID: root1.ID, Page: domain.Page{Limit: 1}},
		{PostID: p1.ID, All: true, Page: domain.Page{After: child1.ID, Limit: 2}},
		{PostID: p1.ID, ParentID: root1.ID, Page: domain.Page{After: child1.ID, Limit: 1}},
		{PostID: p2.ID, Page: domain.Page{Limit: 10}},
		{PostID: p1.ID, ParentID: grandchild.ID, Page: domain.Page{Limit: 1}},
		{PostID: p1.ID, Page: domain.Page{Limit: 1}}, // Duplicate request keeps its slot.
	}
	want := []domain.CommentPage{
		{Items: []domain.Comment{root1}, HasNext: true},
		{Items: []domain.Comment{child1}, HasNext: true},
		{Items: []domain.Comment{grandchild, root2}, HasNext: true},
		{Items: []domain.Comment{child2}},
		{Items: []domain.Comment{other}},
		{Items: []domain.Comment{}},
		{Items: []domain.Comment{root1}, HasNext: true},
	}
	ctx := context.Background()
	counter := &queryCounter{}
	config := store.pool.Config()
	config.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tracedStore := &Store{pool: pool}
	got, err := tracedStore.ListCommentPages(ctx, requests)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("batch pages = %#v, %v; want %#v", got, err, want)
	}
	if count := counter.queries.Load(); count != 1 {
		t.Fatalf("batch used %d queries, want 1", count)
	}
	empty, err := tracedStore.ListCommentPages(ctx, nil)
	if err != nil || len(empty) != 0 || counter.queries.Load() != 1 {
		t.Fatalf("empty batch should not query database: %#v, %v", empty, err)
	}
}

func TestPostgresDisableSerializesWithComment(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	p := createTestPost(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM posts WHERE id = $1 FOR UPDATE`, p.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := store.CreateComment(ctx, domain.NewComment{PostID: p.ID, AuthorID: "reader", Body: "racing comment"})
		done <- err
	}()
	// A completed comment while the owning row is locked would violate atomicity.
	select {
	case err := <-done:
		t.Fatalf("comment completed before permission transaction: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	if _, err := tx.Exec(ctx, `UPDATE posts SET comments_enabled = false WHERE id = $1`, p.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, domain.ErrCommentsDisabled) {
		t.Fatalf("comment after committed disable = %v", err)
	}
	pages, err := store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: p.ID, All: true, Page: domain.Page{Limit: 1}}})
	if err != nil || len(pages[0].Items) != 0 {
		t.Fatalf("disabled post received comment: %#v, %v", pages, err)
	}
}

func TestPostgresConcurrentComments(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	p := createTestPost(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const count = 30
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for range count {
		wg.Go(func() {
			_, err := store.CreateComment(ctx, domain.NewComment{PostID: p.ID, AuthorID: "reader", Body: "concurrent"})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	pages, err := store.ListCommentPages(ctx, []domain.CommentRequest{{PostID: p.ID, All: true, Page: domain.Page{Limit: count}}})
	if err != nil || len(pages[0].Items) != count || pages[0].HasNext {
		t.Fatalf("concurrent page = %#v, %v", pages, err)
	}
	for i := 1; i < count; i++ {
		if pages[0].Items[i-1].ID >= pages[0].Items[i].ID {
			t.Fatal("comments are not strictly ordered")
		}
	}
}

func TestPostgresPostPaginationCannotOvertakeUncommittedInsert(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Pause one insert after it obtains an ID. A second insert must wait rather
	// than become visible with a larger ID that a client could paginate past.
	_, err := store.pool.Exec(ctx, `
		CREATE FUNCTION pause_test_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.title = 'blocked' THEN
				PERFORM pg_advisory_xact_lock(1942317764, hashtext(current_schema()::text));
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER pause_test_insert AFTER INSERT ON posts
		FOR EACH ROW EXECUTE FUNCTION pause_test_insert();
	`)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_xact_lock(1942317764, hashtext(current_schema()::text))`); err != nil {
		t.Fatal(err)
	}
	type insertResult struct {
		post domain.Post
		err  error
	}
	insert := func(title string, done chan<- insertResult) {
		post, err := store.CreatePost(ctx, domain.NewPost{AuthorID: "author", Title: title, Body: "Body", CommentsEnabled: true})
		done <- insertResult{post: post, err: err}
	}
	first, second := make(chan insertResult, 1), make(chan insertResult, 1)
	go insert("blocked", first)
	for {
		var waiting bool
		if err := store.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 1942317764 AND NOT granted
		)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case early := <-first:
			t.Fatalf("first insert did not reach pause trigger: %v", early.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	go insert("second", second)
	select {
	case early := <-second:
		t.Fatalf("later post overtook uncommitted insert: %+v, %v", early.post, early.err)
	case <-time.After(100 * time.Millisecond):
	}
	page, err := store.ListPosts(ctx, domain.Page{Limit: 10})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("forward page exposed a post before earlier insert committed: %+v, %v", page, err)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	one, two := <-first, <-second
	if one.err != nil || two.err != nil || one.post.ID >= two.post.ID {
		t.Fatalf("post inserts out of order: first %+v, second %+v", one, two)
	}
	page, err = store.ListPosts(ctx, domain.Page{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != one.post.ID || !page.HasNext {
		t.Fatalf("first committed page: %+v, %v", page, err)
	}
	page, err = store.ListPosts(ctx, domain.Page{After: one.post.ID, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != two.post.ID || page.HasNext {
		t.Fatalf("second committed page: %+v, %v", page, err)
	}
}

func TestPostgresCanceledEmptyBatch(t *testing.T) {
	// No database is needed: an already canceled call must return the context
	// error even when the request batch would otherwise take a fast return.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&Store{}).ListCommentPages(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled empty batch = %v", err)
	}
}
