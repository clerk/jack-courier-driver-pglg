package pglg

import (
	"embed"
	"io/fs"
)

//go:embed sql/*.sql
var migrationFS embed.FS

// ExportedMigrationFS returns the embedded SQL migration filesystem.
// Used by cmd/pglg-setup to read migration files.
func ExportedMigrationFS() fs.FS {
	return migrationFS
}
