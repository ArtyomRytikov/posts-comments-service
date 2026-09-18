package api

import (
	"context"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api/model"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pagination"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/vikstrous/dataloadgen"
)

type loaderKey struct{}
type actorKey struct{}

func actor(ctx context.Context) string {
	value, _ := ctx.Value(actorKey{}).(string)
	return value
}

func withLoader(ctx context.Context, svc *service.Service) context.Context {
	loader := dataloadgen.NewLoader(func(ctx context.Context, keys []domain.CommentRequest) ([]domain.CommentPage, []error) {
		pages, err := svc.CommentPages(ctx, keys)
		if err == nil {
			return pages, nil
		}
		errs := make([]error, len(keys))
		for i := range errs {
			errs[i] = err
		}
		return make([]domain.CommentPage, len(keys)), errs
	}, dataloadgen.WithWait(time.Millisecond), dataloadgen.WithBatchCapacity(100), dataloadgen.WithoutCache())
	// WithoutCache avoids stale reads after serial mutations and across events on
	// a long-lived subscription. Sibling resolvers still share storage batches.
	return context.WithValue(ctx, loaderKey{}, loader)
}

func loadComments(ctx context.Context, postID, parentID int64, all bool, first int, after *string) (*model.CommentConnection, error) {
	scope := pagination.CommentScope(postID, parentID, all)
	page, err := pagination.Page(first, optionalString(after), scope)
	if err != nil {
		return nil, err
	}
	loader := ctx.Value(loaderKey{}).(*dataloadgen.Loader[domain.CommentRequest, domain.CommentPage])
	result, err := loader.Load(ctx, domain.CommentRequest{PostID: postID, ParentID: parentID, All: all, Page: page})
	if err != nil {
		return nil, err
	}
	return commentConnection(result, scope), nil
}
