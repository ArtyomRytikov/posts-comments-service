package api

import (
	"strconv"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api/model"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pagination"
)

func postModel(p domain.Post) *model.Post {
	return &model.Post{ID: strconv.FormatInt(p.ID, 10), AuthorID: p.AuthorID, Title: p.Title,
		Body: p.Body, CommentsEnabled: p.CommentsEnabled, CreatedAt: p.CreatedAt}
}

func commentModel(c domain.Comment) *model.Comment {
	var parent *string
	if c.ParentID != 0 {
		id := strconv.FormatInt(c.ParentID, 10)
		parent = &id
	}
	return &model.Comment{ID: strconv.FormatInt(c.ID, 10), PostID: strconv.FormatInt(c.PostID, 10),
		ParentID: parent, AuthorID: c.AuthorID, Body: c.Body, CreatedAt: c.CreatedAt}
}

func postConnection(page domain.PostPage) *model.PostConnection {
	edges := make([]*model.PostEdge, 0, len(page.Items))
	var end *string
	for _, post := range page.Items {
		cursor := pagination.Encode(post.ID, pagination.PostsScope)
		edges = append(edges, &model.PostEdge{Cursor: cursor, Node: postModel(post)})
		end = &cursor
	}
	return &model.PostConnection{Edges: edges, PageInfo: &model.PageInfo{HasNextPage: page.HasNext, EndCursor: end}}
}

func commentConnection(page domain.CommentPage, scope string) *model.CommentConnection {
	edges := make([]*model.CommentEdge, 0, len(page.Items))
	var end *string
	for _, comment := range page.Items {
		cursor := pagination.Encode(comment.ID, scope)
		edges = append(edges, &model.CommentEdge{Cursor: cursor, Node: commentModel(comment)})
		end = &cursor
	}
	return &model.CommentConnection{Edges: edges, PageInfo: &model.PageInfo{HasNextPage: page.HasNext, EndCursor: end}}
}

func parseID(value string) (int64, error) {
	// Canonical decimal IDs prevent surprising aliases such as +1 and 01.
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != value {
		return 0, domain.ErrInvalidInput
	}
	return id, nil
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
