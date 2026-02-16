// pglg-setup is a CLI tool for one-time database setup of the pglg outbox
// tables, publication, and replication slot.
//
// Usage:
//
//	pglg-setup create  --conn-string=... --prefix=billing_outbox --publication=billing_outbox_pub --slot=billing_outbox_slot
//	pglg-setup destroy --conn-string=... --prefix=billing_outbox --publication=billing_outbox_pub --slot=billing_outbox_slot
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	pglg "github.com/clerk/jack-courier-driver-pglg"
	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	flags := flag.NewFlagSet(cmd, flag.ExitOnError)
	connString := flags.String("conn-string", "", "PostgreSQL connection string (required)")
	schema := flags.String("schema", "public", "Database schema")
	prefix := flags.String("prefix", "outbox", "Table name prefix")
	publication := flags.String("publication", "", "Publication name (required)")
	slot := flags.String("slot", "", "Replication slot name (required)")
	flags.Parse(os.Args[2:])

	if *connString == "" || *publication == "" || *slot == "" {
		fmt.Fprintln(os.Stderr, "error: --conn-string, --publication, and --slot are required")
		flags.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, *connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	switch cmd {
	case "create":
		if err := runCreate(ctx, conn, *schema, *prefix, *publication, *slot); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("setup complete")
	case "destroy":
		if err := runDestroy(ctx, conn, *schema, *prefix, *publication, *slot); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("teardown complete")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: pglg-setup <create|destroy> [flags]")
	fmt.Fprintln(os.Stderr, "  create   Create outbox tables, publication, and replication slot")
	fmt.Fprintln(os.Stderr, "  destroy  Drop outbox tables, publication, and replication slot")
}

func runCreate(ctx context.Context, conn *pgx.Conn, schema, prefix, publication, slot string) error {
	replacer := strings.NewReplacer(
		"{schema}", schema,
		"{prefix}", prefix,
		"{publication_name}", publication,
		"{slot_name}", slot,
	)

	// Read and execute SQL files in order.
	files, err := readSQLFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		sql := replacer.Replace(f.content)
		fmt.Printf("executing %s...\n", f.name)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
	}

	// Create initial partition covering the current hour.
	now := time.Now().UTC().Truncate(time.Hour)
	upper := now.Add(time.Hour)
	partName := fmt.Sprintf("%s_jobs_%s", prefix, now.Format("20060102_1504"))

	createPartSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s_jobs FOR VALUES FROM ('%s') TO ('%s')",
		schema, partName, schema, prefix,
		now.Format(time.RFC3339), upper.Format(time.RFC3339),
	)
	fmt.Printf("creating initial partition %s...\n", partName)
	if _, err := conn.Exec(ctx, createPartSQL); err != nil {
		return fmt.Errorf("create initial partition: %w", err)
	}

	// Register in partition_meta.
	insertMetaSQL := fmt.Sprintf(
		"INSERT INTO %s.%s_partition_meta (partition_name, lower_bound, upper_bound) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
		schema, prefix,
	)
	if _, err := conn.Exec(ctx, insertMetaSQL, partName, now, upper); err != nil {
		return fmt.Errorf("register initial partition: %w", err)
	}

	return nil
}

func runDestroy(ctx context.Context, conn *pgx.Conn, schema, prefix, publication, slot string) error {
	// Drop replication slot (must be done before dropping publication).
	fmt.Printf("dropping replication slot %s...\n", slot)
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)

	// Drop publication.
	fmt.Printf("dropping publication %s...\n", publication)
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publication))

	// Drop tables (cascade drops partitions).
	tables := []string{
		fmt.Sprintf("%s.%s_jobs", schema, prefix),
		fmt.Sprintf("%s.%s_cursor", schema, prefix),
		fmt.Sprintf("%s.%s_partition_meta", schema, prefix),
	}
	for _, t := range tables {
		fmt.Printf("dropping table %s...\n", t)
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t)); err != nil {
			return fmt.Errorf("drop %s: %w", t, err)
		}
	}

	return nil
}

type sqlFile struct {
	name    string
	content string
}

func readSQLFiles() ([]sqlFile, error) {
	entries, err := fs.ReadDir(pglg.ExportedMigrationFS(), "sql")
	if err != nil {
		return nil, fmt.Errorf("read migration dir: %w", err)
	}

	var files []sqlFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		content, err := fs.ReadFile(pglg.ExportedMigrationFS(), "sql/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		files = append(files, sqlFile{name: e.Name(), content: string(content)})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].name < files[j].name
	})

	return files, nil
}
