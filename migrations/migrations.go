package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

//go:embed platform/*.sql orchestrator/*.sql inventory/*.sql payment/*.sql
var files embed.FS

// Services that own a database.
var Services = []string{"orchestrator", "inventory", "payment"}

// Up applies platform and service migrations to db.
func Up(ctx context.Context, db *sql.DB, service string, log *slog.Logger) error {
	sets := []struct{ dir, table string }{
		{"platform", "goose_platform_version"},
		{service, "goose_db_version"},
	}
	for _, s := range sets {
		if err := up(ctx, db, s.dir, s.table, log.With("service", service, "set", s.dir)); err != nil {
			return fmt.Errorf("migrate %s/%s: %w", service, s.dir, err)
		}
	}
	return nil
}

func up(ctx context.Context, db *sql.DB, dir, table string, log *slog.Logger) error {
	fsys, err := fs.Sub(files, dir)
	if err != nil {
		return err
	}
	store, err := database.NewStore(database.DialectPostgres, table)
	if err != nil {
		return err
	}
	p, err := goose.NewProvider("", db, fsys, goose.WithStore(store))
	if err != nil {
		return err
	}
	results, err := p.Up(ctx)
	if err != nil {
		return err
	}
	for _, r := range results {
		log.Info("migration applied", "version", r.Source.Version, "file", r.Source.Path, "took", r.Duration)
	}
	if len(results) == 0 {
		log.Info("schema up to date")
	}
	return nil
}
