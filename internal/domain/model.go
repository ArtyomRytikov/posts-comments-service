// Package domain contains storage-independent models and errors.
package domain

import (
	"errors"
	"time"
)

const MaxCommentLength = 2000
const MaxPageSize = 100

var (
	ErrNotFound         = errors.New("post not found")
	ErrParentNotFound   = errors.New("parent comment not found in this post")
	ErrCommentsDisabled = errors.New("comments are disabled for this post")
	ErrForbidden        = errors.New("only the post author can change comment permissions")
	ErrUnauthenticated  = errors.New("X-User-ID header is required")
	ErrInvalidInput     = errors.New("invalid input")
	ErrInvalidCursor    = errors.New("invalid cursor for this connection")
	ErrInvalidPage      = errors.New("first must be between 1 and 100")
	ErrCommentTooLong   = errors.New("comment must contain at most 2000 characters")
)

type Post struct {
	ID              int64
	AuthorID        string
	Title           string
	Body            string
	CommentsEnabled bool
	CreatedAt       time.Time
}

type Comment struct {
	ID        int64
	PostID    int64
	ParentID  int64 // Zero denotes a root comment.
	AuthorID  string
	Body      string
	CreatedAt time.Time
}

type NewPost struct {
	AuthorID        string
	Title           string
	Body            string
	CommentsEnabled bool
}

type NewComment struct {
	PostID   int64
	ParentID int64
	AuthorID string
	Body     string
}

// Page is an ascending, exclusive keyset window. Limit is in [1, MaxPageSize].
type Page struct {
	After int64
	Limit int
}
type PostPage struct {
	Items   []Post
	HasNext bool
}
type CommentPage struct {
	Items   []Comment
	HasNext bool
}

// CommentRequest is comparable and can be used as a DataLoader key.
// All selects the flat post feed; otherwise ParentID selects immediate children.
type CommentRequest struct {
	PostID   int64
	ParentID int64
	All      bool
	Page     Page
}
