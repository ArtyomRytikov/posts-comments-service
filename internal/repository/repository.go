// Package repository defines the persistence contract used by the service.
package repository

import (
	"context"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
)

// Repository implementations return independent values, never shared mutable
// pointers. Authorization and the comments-enabled check must be atomic with
// their respective writes. Comment parents must belong to the same post.
type Repository interface {
	CreatePost(context.Context, domain.NewPost) (domain.Post, error)
	GetPost(context.Context, int64) (domain.Post, error)
	ListPosts(context.Context, domain.Page) (domain.PostPage, error)
	SetCommentsEnabled(context.Context, int64, string, bool) (domain.Post, error)
	CreateComment(context.Context, domain.NewComment) (domain.Comment, error)
	// ListCommentPages returns one page per request, in the same order. It must
	// batch storage access, not issue one database round trip per request.
	// The caller verifies that the owning post exists.
	ListCommentPages(context.Context, []domain.CommentRequest) ([]domain.CommentPage, error)
}
