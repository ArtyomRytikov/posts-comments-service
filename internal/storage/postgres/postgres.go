// Package postgres implements persistent, transactional post and comment storage.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

var _ repository.Repository = (*Store)(nil)

// New connects to PostgreSQL. Schema changes are applied separately by Migrate.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("configure postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const postColumns = `id, author_id, title, body, comments_enabled, created_at`

func scanPost(row pgx.Row) (domain.Post, error) {
	var p domain.Post
	err := row.Scan(&p.ID, &p.AuthorID, &p.Title, &p.Body, &p.CommentsEnabled, &p.CreatedAt)
	return p, err
}

func (s *Store) CreatePost(ctx context.Context, in domain.NewPost) (domain.Post, error) {
	// The transaction-level lock is acquired before the identity value is
	// allocated, so forward pagination cannot skip an earlier uncommitted post.
	// Scope it to this schema and use the two-int advisory-lock namespace, which
	// is separate from the migration runner's bigint lock namespace.
	p, err := scanPost(s.pool.QueryRow(ctx, `WITH insertion_lock AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(1648110923, hashtext(current_schema()::text))
	)
		INSERT INTO posts (author_id, title, body, comments_enabled)
		SELECT $1, $2, $3, $4 FROM insertion_lock
		RETURNING `+postColumns, in.AuthorID, in.Title, in.Body, in.CommentsEnabled))
	return p, mapError(err)
}

func (s *Store) GetPost(ctx context.Context, id int64) (domain.Post, error) {
	p, err := scanPost(s.pool.QueryRow(ctx, `SELECT `+postColumns+` FROM posts WHERE id = $1`, id))
	return p, mapError(err)
}

func validatePage(page domain.Page) error {
	if page.Limit < 1 || page.Limit > domain.MaxPageSize {
		return domain.ErrInvalidPage
	}
	if page.After < 0 {
		return domain.ErrInvalidCursor
	}
	return nil
}

func (s *Store) ListPosts(ctx context.Context, page domain.Page) (domain.PostPage, error) {
	result := domain.PostPage{Items: []domain.Post{}}
	if err := validatePage(page); err != nil {
		return result, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+postColumns+`
		FROM posts WHERE id > $1 ORDER BY id LIMIT $2`, page.After, page.Limit+1)
	if err != nil {
		return result, mapError(err)
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanPost(rows)
		if err != nil {
			return result, mapError(err)
		}
		result.Items = append(result.Items, p)
	}
	if err := rows.Err(); err != nil {
		return result, mapError(err)
	}
	if len(result.Items) > page.Limit {
		result.HasNext = true
		result.Items = result.Items[:page.Limit]
	}
	return result, nil
}

// SetCommentsEnabled locks the same row as CreateComment, so an acknowledged
// disable prevents later comment writes even across multiple server processes.
func (s *Store) SetCommentsEnabled(ctx context.Context, id int64, authorID string, enabled bool) (domain.Post, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Post{}, mapError(err)
	}
	defer tx.Rollback(ctx)
	p, err := scanPost(tx.QueryRow(ctx, `SELECT `+postColumns+` FROM posts WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return domain.Post{}, mapError(err)
	}
	if p.AuthorID != authorID {
		return domain.Post{}, domain.ErrForbidden
	}
	p, err = scanPost(tx.QueryRow(ctx, `UPDATE posts SET comments_enabled = $2 WHERE id = $1 RETURNING `+postColumns, id, enabled))
	if err != nil {
		return domain.Post{}, mapError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Post{}, mapError(err)
	}
	return p, nil
}

func (s *Store) CreateComment(ctx context.Context, in domain.NewComment) (domain.Comment, error) {
	if !utf8.ValidString(in.Body) || in.ParentID < 0 {
		return domain.Comment{}, domain.ErrInvalidInput
	}
	if utf8.RuneCountInString(in.Body) > domain.MaxCommentLength {
		return domain.Comment{}, domain.ErrCommentTooLong
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Comment{}, mapError(err)
	}
	defer tx.Rollback(ctx)
	var enabled bool
	// Acquire the owning row before allocating the comment ID: successful writes
	// commit in ID order for each post, keeping forward pagination stable.
	if err := tx.QueryRow(ctx, `SELECT comments_enabled FROM posts WHERE id = $1 FOR UPDATE`, in.PostID).Scan(&enabled); err != nil {
		return domain.Comment{}, mapError(err)
	}
	if !enabled {
		return domain.Comment{}, domain.ErrCommentsDisabled
	}
	var c domain.Comment
	err = tx.QueryRow(ctx, `INSERT INTO comments (post_id, parent_id, author_id, body)
		SELECT $1, NULLIF($2::bigint, 0), $3, $4
		WHERE $2::bigint = 0 OR EXISTS (SELECT 1 FROM comments WHERE post_id = $1 AND id = $2)
		RETURNING id, post_id, COALESCE(parent_id, 0), author_id, body, created_at`,
		in.PostID, in.ParentID, in.AuthorID, in.Body).Scan(&c.ID, &c.PostID, &c.ParentID, &c.AuthorID, &c.Body, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Comment{}, domain.ErrParentNotFound
	}
	if err != nil {
		return domain.Comment{}, mapError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Comment{}, mapError(err)
	}
	return c, nil
}

// ListCommentPages batches independent keyset windows into one database round
// trip. Separate branches allow PostgreSQL to use the matching composite index
// for the flat feed, root comments, and a particular parent's immediate replies.
func (s *Store) ListCommentPages(ctx context.Context, requests []domain.CommentRequest) ([]domain.CommentPage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.CommentPage, len(requests))
	if len(requests) == 0 {
		return result, nil
	}
	postIDs := make([]int64, len(requests))
	parentIDs := make([]int64, len(requests))
	all := make([]bool, len(requests))
	afterIDs := make([]int64, len(requests))
	limits := make([]int32, len(requests))
	for i, request := range requests {
		if err := validatePage(request.Page); err != nil {
			return nil, err
		}
		if request.ParentID < 0 {
			return nil, domain.ErrInvalidInput
		}
		postIDs[i], parentIDs[i], all[i] = request.PostID, request.ParentID, request.All
		afterIDs[i], limits[i] = request.Page.After, int32(request.Page.Limit)
		result[i].Items = []domain.Comment{}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.ordinality, c.id, c.post_id, COALESCE(c.parent_id, 0), c.author_id, c.body, c.created_at
		FROM unnest($1::bigint[], $2::bigint[], $3::boolean[], $4::bigint[], $5::integer[])
			WITH ORDINALITY AS r(post_id, parent_id, all_comments, after_id, page_limit, ordinality)
		CROSS JOIN LATERAL (
			(SELECT id, post_id, parent_id, author_id, body, created_at FROM comments
			 WHERE r.all_comments AND post_id = r.post_id AND id > r.after_id
			 ORDER BY id LIMIT r.page_limit + 1)
			UNION ALL
			(SELECT id, post_id, parent_id, author_id, body, created_at FROM comments
			 WHERE NOT r.all_comments AND r.parent_id = 0 AND post_id = r.post_id
				AND parent_id IS NULL AND id > r.after_id
			 ORDER BY id LIMIT r.page_limit + 1)
			UNION ALL
			(SELECT id, post_id, parent_id, author_id, body, created_at FROM comments
			 WHERE NOT r.all_comments AND r.parent_id <> 0 AND post_id = r.post_id
				AND parent_id = r.parent_id AND id > r.after_id
			 ORDER BY id LIMIT r.page_limit + 1)
		) AS c
		ORDER BY r.ordinality, c.id`, postIDs, parentIDs, all, afterIDs, limits)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal int64
		var c domain.Comment
		if err := rows.Scan(&ordinal, &c.ID, &c.PostID, &c.ParentID, &c.AuthorID, &c.Body, &c.CreatedAt); err != nil {
			return nil, mapError(err)
		}
		index := int(ordinal - 1)
		if len(result[index].Items) == requests[index].Page.Limit {
			result[index].HasNext = true
		} else {
			result[index].Items = append(result[index].Items, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, mapError(err)
	}
	return result, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "23503" && pgErr.ConstraintName == "comments_parent_same_post":
			return domain.ErrParentNotFound
		case pgErr.Code == "23514" && pgErr.ConstraintName == "comments_body_length":
			return domain.ErrCommentTooLong
		case pgErr.Code == "23514" || pgErr.Code == "22021":
			return domain.ErrInvalidInput
		}
	}
	// The API boundary logs internal errors and exposes only a generic message.
	return fmt.Errorf("postgres operation: %w", err)
}
