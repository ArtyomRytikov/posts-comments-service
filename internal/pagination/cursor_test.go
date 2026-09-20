package pagination

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/ArtyomRytikov/posts-comments-service/internal/domain"
)

func TestCursorScopeAndValidation(t *testing.T) {
	scope := CommentScope(2, 3, false)
	cursor := Encode(42, scope)
	if id, err := Decode(cursor, scope); err != nil || id != 42 {
		t.Fatalf("round trip: %d %v", id, err)
	}
	for _, c := range []string{cursor, "broken!", base64.RawURLEncoding.EncodeToString([]byte("v2|posts|1")), Encode(-1, PostsScope), base64.RawURLEncoding.EncodeToString([]byte("v1|posts|01"))} {
		if _, err := Decode(c, PostsScope); !errors.Is(err, domain.ErrInvalidCursor) {
			t.Errorf("accepted %q: %v", c, err)
		}
	}
	if _, err := Decode(cursor, CommentScope(2, 4, false)); !errors.Is(err, domain.ErrInvalidCursor) {
		t.Fatal("cursor reused for another parent")
	}
	if _, err := Decode(cursor, CommentScope(2, 3, true)); !errors.Is(err, domain.ErrInvalidCursor) {
		t.Fatal("cursor reused for flat feed")
	}
	for _, n := range []int{-1, 0, 101} {
		if _, err := Page(n, "", PostsScope); !errors.Is(err, domain.ErrInvalidPage) {
			t.Errorf("size %d: %v", n, err)
		}
	}
	if p, err := Page(20, "", PostsScope); err != nil || p.After != 0 || p.Limit != 20 {
		t.Fatalf("default: %+v %v", p, err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add("!")
	f.Add(Encode(1, PostsScope))
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		id, err := Decode(value, PostsScope)
		if err == nil && id < 0 {
			t.Fatal("negative cursor")
		}
	})
}

// Strict Base64 decoding still ignores CR/LF; cursors must have one canonical
// wire representation rather than accepting silently modified input.
func TestCursorRejectsEmbeddedNewlines(t *testing.T) {
	cursor := Encode(42, PostsScope)
	for _, invalid := range []string{cursor + "\n", "\r" + cursor, cursor[:4] + "\r\n" + cursor[4:]} {
		if _, err := Decode(invalid, PostsScope); !errors.Is(err, domain.ErrInvalidCursor) {
			t.Errorf("accepted noncanonical cursor %q: %v", invalid, err)
		}
	}
}
