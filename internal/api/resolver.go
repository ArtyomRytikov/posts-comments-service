package api

import "github.com/ArtyomRytikov/posts-comments-service/internal/service"

// Resolver owns the application dependencies, not per-request state.
type Resolver struct {
	service *service.Service
}
