package config

import "testing"

func TestLoad(t *testing.T) {
	for _, key := range []string{"STORAGE", "DATABASE_URL", "AUTO_MIGRATE", "HTTP_ADDR"} {
		t.Setenv(key, "")
	}
	c, err := Load()
	if err != nil || c.Storage != "memory" || c.Address != ":8080" || !c.AutoMigrate {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	t.Setenv("STORAGE", "unknown")
	if _, err := Load(); err == nil {
		t.Fatal("invalid storage")
	}
	t.Setenv("STORAGE", "postgres")
	if _, err := Load(); err == nil {
		t.Fatal("missing DSN")
	}
	t.Setenv("DATABASE_URL", "postgres://example")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AUTO_MIGRATE", "bad")
	if _, err := Load(); err == nil {
		t.Fatal("invalid bool")
	}
}
