package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate applies pending embedded migrations in one transaction. An advisory
// lock serializes concurrent startup, including creation of the version table.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(73661838416284201)`); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS posts_comments_schema_migrations (
		version bigint PRIMARY KEY,
		name text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create migration history: %w", err)
	}
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(files)
	for _, name := range files {
		base := strings.TrimPrefix(name, "migrations/")
		versionText, _, ok := strings.Cut(base, "_")
		if !ok {
			return fmt.Errorf("invalid migration name %q", name)
		}
		version, err := strconv.ParseInt(versionText, 10, 64)
		if err != nil || version <= 0 {
			return fmt.Errorf("invalid migration version %q", name)
		}
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM posts_comments_schema_migrations WHERE version = $1
		)`, version).Scan(&applied); err != nil {
			return fmt.Errorf("read migration history: %w", err)
		}
		if applied {
			continue
		}
		body, err := migrationFiles.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", base, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", base, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO posts_comments_schema_migrations (version, name) VALUES ($1, $2)`, version, base); err != nil {
			return fmt.Errorf("record migration %s: %w", base, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}
