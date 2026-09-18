// Package memory implements the repository with indexed, process-local storage.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
)

type childrenKey struct {
	postID   int64
	parentID int64
}

// Store is safe for concurrent use. IDs grow monotonically, so appending to the
// indexes preserves their order without sorting or walking comment subtrees.
type Store struct {
	mu            sync.RWMutex
	nextPostID    int64
	nextCommentID int64
	posts         map[int64]domain.Post
	comments      map[int64]domain.Comment
	postIDs       []int64
	postComments  map[int64][]int64
	children      map[childrenKey][]int64
}

var _ repository.Repository = (*Store)(nil)

func New() *Store {
	return &Store{
		posts:        make(map[int64]domain.Post),
		comments:     make(map[int64]domain.Comment),
		postComments: make(map[int64][]int64),
		children:     make(map[childrenKey][]int64),
	}
}

func (s *Store) CreatePost(ctx context.Context, input domain.NewPost) (domain.Post, error) {
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	s.nextPostID++
	post := domain.Post{
		ID: s.nextPostID, AuthorID: input.AuthorID, Title: input.Title,
		Body: input.Body, CommentsEnabled: input.CommentsEnabled,
		CreatedAt: time.Now().UTC(),
	}
	s.posts[post.ID] = post
	s.postIDs = append(s.postIDs, post.ID)
	return post, nil
}

func (s *Store) GetPost(ctx context.Context, id int64) (domain.Post, error) {
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	post, ok := s.posts[id]
	if !ok {
		return domain.Post{}, domain.ErrNotFound
	}
	return post, nil
}

func (s *Store) ListPosts(ctx context.Context, page domain.Page) (domain.PostPage, error) {
	if err := ctx.Err(); err != nil {
		return domain.PostPage{}, err
	}
	if err := validatePage(page); err != nil {
		return domain.PostPage{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return domain.PostPage{}, err
	}
	ids, hasNext := window(s.postIDs, page)
	result := domain.PostPage{Items: make([]domain.Post, len(ids)), HasNext: hasNext}
	for i, id := range ids {
		result.Items[i] = s.posts[id]
	}
	return result, nil
}

func (s *Store) SetCommentsEnabled(ctx context.Context, id int64, authorID string, enabled bool) (domain.Post, error) {
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.Post{}, err
	}
	post, ok := s.posts[id]
	if !ok {
		return domain.Post{}, domain.ErrNotFound
	}
	if post.AuthorID != authorID {
		return domain.Post{}, domain.ErrForbidden
	}
	post.CommentsEnabled = enabled
	s.posts[id] = post
	return post, nil
}

func (s *Store) CreateComment(ctx context.Context, input domain.NewComment) (domain.Comment, error) {
	if err := ctx.Err(); err != nil {
		return domain.Comment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.Comment{}, err
	}
	post, ok := s.posts[input.PostID]
	if !ok {
		return domain.Comment{}, domain.ErrNotFound
	}
	if !post.CommentsEnabled {
		return domain.Comment{}, domain.ErrCommentsDisabled
	}
	if input.ParentID != 0 {
		parent, exists := s.comments[input.ParentID]
		if !exists || parent.PostID != input.PostID {
			return domain.Comment{}, domain.ErrParentNotFound
		}
	}
	s.nextCommentID++
	comment := domain.Comment{
		ID: s.nextCommentID, PostID: input.PostID, ParentID: input.ParentID,
		AuthorID: input.AuthorID, Body: input.Body, CreatedAt: time.Now().UTC(),
	}
	s.comments[comment.ID] = comment
	s.postComments[comment.PostID] = append(s.postComments[comment.PostID], comment.ID)
	key := childrenKey{postID: comment.PostID, parentID: comment.ParentID}
	s.children[key] = append(s.children[key], comment.ID)
	return comment, nil
}

func (s *Store) ListCommentPages(ctx context.Context, requests []domain.CommentRequest) ([]domain.CommentPage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, request := range requests {
		if err := validatePage(request.Page); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.CommentPage, len(requests))
	for i, request := range requests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ids := s.postComments[request.PostID]
		if !request.All {
			ids = s.children[childrenKey{postID: request.PostID, parentID: request.ParentID}]
		}
		ids, hasNext := window(ids, request.Page)
		result[i] = domain.CommentPage{Items: make([]domain.Comment, len(ids)), HasNext: hasNext}
		for j, id := range ids {
			result[i].Items[j] = s.comments[id]
		}
	}
	return result, nil
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

// window uses binary search, making page access O(log n + page size) even at
// large offsets. Callers hold the store lock while using this index slice.
func window(ids []int64, page domain.Page) ([]int64, bool) {
	start := sort.Search(len(ids), func(i int) bool { return ids[i] > page.After })
	remaining := len(ids) - start
	if remaining > page.Limit {
		return ids[start : start+page.Limit], true
	}
	return ids[start:], false
}
