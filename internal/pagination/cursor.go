// Package pagination implements versioned cursors bound to a connection.
package pagination

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
)

const PostsScope = "posts"

func CommentScope(postID, parentID int64, all bool) string {
	return fmt.Sprintf("comments:%d:%d:%t", postID, parentID, all)
}

func Encode(id int64, scope string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("v1|%s|%d", scope, id)))
}

func Decode(cursor, scope string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	if len(cursor) > 256 {
		return 0, domain.ErrInvalidCursor
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
	if err != nil {
		return 0, domain.ErrInvalidCursor
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 3 || parts[0] != "v1" || parts[1] != scope {
		return 0, domain.ErrInvalidCursor
	}
	id, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != parts[2] {
		return 0, domain.ErrInvalidCursor
	}
	return id, nil
}

func Page(first int, after, scope string) (domain.Page, error) {
	if first < 1 || first > domain.MaxPageSize {
		return domain.Page{}, domain.ErrInvalidPage
	}
	id, err := Decode(after, scope)
	if err != nil {
		return domain.Page{}, err
	}
	return domain.Page{After: id, Limit: first}, nil
}
