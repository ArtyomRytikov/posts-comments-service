// Package service validates use cases and coordinates persistence and events.
package service

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
)

type Events interface {
	Publish(domain.Comment)
	Subscribe(context.Context, int64) <-chan domain.Comment
}

type Service struct {
	repo   repository.Repository
	events Events
}

func New(repo repository.Repository, events Events) *Service {
	return &Service{repo: repo, events: events}
}

func (s *Service) CreatePost(ctx context.Context, actor, title, body string, enabled bool) (domain.Post, error) {
	if err := validateActor(actor); err != nil {
		return domain.Post{}, err
	}
	if !validText(title, 200) || !validText(body, 100000) {
		return domain.Post{}, fmt.Errorf("%w: title (1..200) and body (1..100000) must contain text", domain.ErrInvalidInput)
	}
	return s.repo.CreatePost(ctx, domain.NewPost{AuthorID: actor, Title: title, Body: body, CommentsEnabled: enabled})
}

func (s *Service) GetPost(ctx context.Context, id int64) (domain.Post, error) {
	if id <= 0 {
		return domain.Post{}, fmt.Errorf("%w: post ID must be positive", domain.ErrInvalidInput)
	}
	return s.repo.GetPost(ctx, id)
}

func (s *Service) Posts(ctx context.Context, page domain.Page) (domain.PostPage, error) {
	if err := validatePage(page); err != nil {
		return domain.PostPage{}, err
	}
	return s.repo.ListPosts(ctx, page)
}

func (s *Service) SetCommentsEnabled(ctx context.Context, postID int64, actor string, enabled bool) (domain.Post, error) {
	if err := validateActor(actor); err != nil {
		return domain.Post{}, err
	}
	if postID <= 0 {
		return domain.Post{}, fmt.Errorf("%w: post ID must be positive", domain.ErrInvalidInput)
	}
	return s.repo.SetCommentsEnabled(ctx, postID, actor, enabled)
}

func (s *Service) CreateComment(ctx context.Context, actor string, postID, parentID int64, body string) (domain.Comment, error) {
	if err := validateActor(actor); err != nil {
		return domain.Comment{}, err
	}
	if postID <= 0 || parentID < 0 {
		return domain.Comment{}, fmt.Errorf("%w: invalid post or parent ID", domain.ErrInvalidInput)
	}
	if utf8.RuneCountInString(body) > domain.MaxCommentLength {
		return domain.Comment{}, domain.ErrCommentTooLong
	}
	if !validText(body, domain.MaxCommentLength) {
		return domain.Comment{}, fmt.Errorf("%w: comment must contain valid text", domain.ErrInvalidInput)
	}
	c, err := s.repo.CreateComment(ctx, domain.NewComment{AuthorID: actor, PostID: postID, ParentID: parentID, Body: body})
	if err != nil {
		return domain.Comment{}, err
	}
	s.events.Publish(c)
	return c, nil
}

func (s *Service) CommentPages(ctx context.Context, requests []domain.CommentRequest) ([]domain.CommentPage, error) {
	for _, r := range requests {
		if r.PostID <= 0 || r.ParentID < 0 || (r.All && r.ParentID != 0) {
			return nil, domain.ErrInvalidInput
		}
		if err := validatePage(r.Page); err != nil {
			return nil, err
		}
	}
	return s.repo.ListCommentPages(ctx, requests)
}

func (s *Service) Subscribe(ctx context.Context, postID int64) (<-chan domain.Comment, error) {
	if _, err := s.GetPost(ctx, postID); err != nil {
		return nil, err
	}
	return s.events.Subscribe(ctx, postID), nil
}

func validText(text string, max int) bool {
	return utf8.ValidString(text) && !strings.ContainsRune(text, 0) && strings.TrimSpace(text) != "" && utf8.RuneCountInString(text) <= max
}

func validateActor(actor string) error {
	if actor == "" {
		return domain.ErrUnauthenticated
	}
	if !validText(actor, 128) || strings.TrimSpace(actor) != actor {
		return fmt.Errorf("%w: invalid user ID", domain.ErrInvalidInput)
	}
	return nil
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
