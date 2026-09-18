// Package api exposes the service as a bounded GraphQL HTTP/WebSocket endpoint.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/ArtyomRytikov/posts-comments-service/internal/api/generated"
	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/gorilla/websocket"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const (
	maxBodyBytes     = 1 << 20
	maxComplexity    = 10000
	maxQueryDepth    = 20
	operationTimeout = 10 * time.Second
)

func New(svc *service.Service, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	cfg := generated.Config{Resolvers: &Resolver{service: svc}}
	cfg.Complexity.Query.Posts = connectionComplexity
	cfg.Complexity.Post.Comments = connectionComplexity
	cfg.Complexity.Post.CommentFeed = connectionComplexity
	cfg.Complexity.Comment.Replies = connectionComplexity
	server := handler.New(generated.NewExecutableSchema(cfg))
	server.AddTransport(transport.Websocket{
		// The default Gorilla origin check accepts absent Origin headers and
		// otherwise requires the Origin host to match the request host.
		Upgrader:         websocket.Upgrader{HandshakeTimeout: 5 * time.Second},
		InitTimeout:      5 * time.Second,
		PingPongInterval: 20 * time.Second,
	})
	server.AddTransport(transport.Options{})
	server.AddTransport(transport.GET{})
	server.AddTransport(transport.POST{})
	server.SetQueryCache(lru.New[*ast.QueryDocument](256))
	server.SetParserTokenLimit(10000)
	server.Use(operationLimits{})
	server.Use(extension.FixedComplexityLimit(maxComplexity))
	server.Use(extension.Introspection{})
	server.SetErrorPresenter(errorPresenter(logger))
	server.SetRecoverFunc(func(ctx context.Context, recovered any) error {
		logger.ErrorContext(ctx, "panic in GraphQL operation", "panic", recovered)
		return errors.New("internal server error")
	})
	server.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		ctx = withLoader(ctx, svc)
		if graphql.GetOperationContext(ctx).Operation.Operation == ast.Subscription {
			return next(ctx)
		}
		ctx, cancel := context.WithTimeout(ctx, operationTimeout)
		response := next(ctx)
		return func(responseCtx context.Context) *graphql.Response {
			defer cancel()
			return response(responseCtx)
		}
	})
	mux := http.NewServeMux()
	mux.Handle("/query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		ctx := context.WithValue(r.Context(), actorKey{}, r.Header.Get("X-User-ID"))
		if websocket.IsWebSocketUpgrade(r) {
			w = deadlineWriter{w}
		}
		server.ServeHTTP(w, r.WithContext(ctx))
	}))
	mux.Handle("GET /{$}", playground.Handler("Posts and comments", "/query"))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

func connectionComplexity(childComplexity, first int, _ *string) int {
	// Saturate to prevent integer overflow before validation rejects the query.
	if first < 1 || first > domain.MaxPageSize || childComplexity > maxComplexity/first {
		return maxComplexity + 1
	}
	return 1 + first*childComplexity
}

func errorPresenter(logger *slog.Logger) graphql.ErrorPresenterFunc {
	return func(ctx context.Context, err error) *gqlerror.Error {
		presented := graphql.DefaultErrorPresenter(ctx, err)
		code, message := "INTERNAL_SERVER_ERROR", "internal server error"
		switch {
		case errors.Is(err, domain.ErrNotFound):
			code, message = "NOT_FOUND", domain.ErrNotFound.Error()
		case errors.Is(err, domain.ErrParentNotFound):
			code, message = "BAD_USER_INPUT", domain.ErrParentNotFound.Error()
		case errors.Is(err, domain.ErrForbidden):
			code, message = "FORBIDDEN", domain.ErrForbidden.Error()
		case errors.Is(err, domain.ErrUnauthenticated):
			code, message = "UNAUTHENTICATED", domain.ErrUnauthenticated.Error()
		case errors.Is(err, domain.ErrCommentsDisabled):
			code, message = "COMMENTS_DISABLED", domain.ErrCommentsDisabled.Error()
		case errors.Is(err, domain.ErrInvalidInput):
			code, message = "BAD_USER_INPUT", domain.ErrInvalidInput.Error()
		case errors.Is(err, domain.ErrInvalidCursor):
			code, message = "BAD_USER_INPUT", domain.ErrInvalidCursor.Error()
		case errors.Is(err, domain.ErrInvalidPage):
			code, message = "BAD_USER_INPUT", domain.ErrInvalidPage.Error()
		case errors.Is(err, domain.ErrCommentTooLong):
			code, message = "BAD_USER_INPUT", domain.ErrCommentTooLong.Error()
		case errors.Is(err, context.DeadlineExceeded):
			message = "request timed out"
		default:
			switch presented.Extensions["code"] {
			case "GRAPHQL_PARSE_FAILED", "GRAPHQL_VALIDATION_FAILED", "COMPLEXITY_LIMIT_EXCEEDED", "BAD_USER_INPUT":
				code, message = "BAD_USER_INPUT", presented.Message
			}
		}
		if code == "INTERNAL_SERVER_ERROR" {
			logger.ErrorContext(ctx, "GraphQL operation failed", "error", err)
		}
		return &gqlerror.Error{Message: message, Path: presented.Path, Locations: presented.Locations,
			Extensions: map[string]any{"code": code}}
	}
}
