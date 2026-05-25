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

	"github.com/jackc/pgx/v5"

	pglg "github.com/clerk/jack-courier-driver-pglg"
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
	publication := flags.String("publication", "", "Publication name (required unless --replica)")
	slot := flags.String("slot", "", "Replication slot name (required unless --replica)")
	skipPublication := flags.Bool("skip-publication", false, "Skip publication creation (and the CREATE-on-database privilege check); use when the publication is managed by a more-privileged role")
	replica := flags.Bool("replica", false, "Bootstrap a logical replica: creates schema only, skips publication and replication-slot creation and their privilege checks; --publication and --slot are not required")
	createPartitionsFrom := flags.String("create-partitions", "", "RFC3339 timestamp; only valid with --replica. Creates hourly partitions from this time through now+13h (idempotent). Use to back-fill historical partitions on a replica so the apply worker can drain WAL referencing partitions the primary has already dropped")
	flags.Parse(os.Args[2:])

	if *connString == "" {
		fmt.Fprintln(os.Stderr, "error: --conn-string is required")
		flags.Usage()
		os.Exit(1)
	}

	if cmd != "create" {
		if *replica {
			fmt.Fprintf(os.Stderr, "error: --replica is only supported for 'create' (got %q)\n", cmd)
			os.Exit(1)
		}
		if *createPartitionsFrom != "" {
			fmt.Fprintf(os.Stderr, "error: --create-partitions is only supported for 'create' (got %q)\n", cmd)
			os.Exit(1)
		}
	}

	if *createPartitionsFrom != "" && !*replica {
		fmt.Fprintln(os.Stderr, "error: --create-partitions requires --replica")
		flags.Usage()
		os.Exit(1)
	}

	pubSlotRequired := cmd != "create" || !*replica
	if pubSlotRequired && (*publication == "" || *slot == "") {
		if cmd == "create" {
			fmt.Fprintln(os.Stderr, "error: --publication and --slot are required (unless --replica)")
		} else {
			fmt.Fprintln(os.Stderr, "error: --publication and --slot are required")
		}
		flags.Usage()
		os.Exit(1)
	}

	var partitionsStart time.Time
	if *createPartitionsFrom != "" {
		var err error
		partitionsStart, err = time.Parse(time.RFC3339, *createPartitionsFrom)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --create-partitions: invalid RFC3339 timestamp: %v\n", err)
			os.Exit(1)
		}
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
		if err := runCreate(ctx, conn, *schema, *prefix, *publication, *slot, *skipPublication, *replica, partitionsStart); err != nil {
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

// checkPrivileges verifies the connected user has the necessary privileges
// to create tables, publications, and replication slots. When
// skipPublication is true, the CREATE-on-database check is omitted. When
// skipSlot is true, the REPLICATION-role check is omitted.
func checkPrivileges(ctx context.Context, conn *pgx.Conn, schema string, skipPublication, skipSlot bool) error {
	var user string
	if err := conn.QueryRow(ctx, "SELECT current_user").Scan(&user); err != nil {
		return fmt.Errorf("get current user: %w", err)
	}
	fmt.Printf("connected as user: %s\n", user)

	var missing []string

	if !skipSlot {
		var hasReplication bool
		err := conn.QueryRow(ctx,
			"SELECT rolreplication FROM pg_roles WHERE rolname = current_user",
		).Scan(&hasReplication)
		if err != nil {
			return fmt.Errorf("check replication role: %w", err)
		}
		if !hasReplication {
			missing = append(missing, "REPLICATION role (required to create replication slots)")
		}
	}

	var hasSchemaCreate bool
	if err := conn.QueryRow(ctx,
		"SELECT has_schema_privilege(current_user, $1, 'CREATE')", schema,
	).Scan(&hasSchemaCreate); err != nil {
		return fmt.Errorf("check schema privilege: %w", err)
	}
	if !hasSchemaCreate {
		missing = append(missing, fmt.Sprintf("CREATE on schema %q (required to create tables)", schema))
	}

	if !skipPublication {
		var hasDBCreate bool
		if err := conn.QueryRow(ctx,
			"SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
		).Scan(&hasDBCreate); err != nil {
			return fmt.Errorf("check database privilege: %w", err)
		}
		if !hasDBCreate {
			missing = append(missing, "CREATE on database (required to create publications)")
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("user %q is missing required privileges:\n  - %s", user, strings.Join(missing, "\n  - "))
	}

	fmt.Println("privilege check passed")
	return nil
}

func runCreate(ctx context.Context, conn *pgx.Conn, schema, prefix, publication, slot string, skipPublication, replica bool, partitionsStart time.Time) error {
	skipPub := skipPublication || replica
	skipSlot := replica

	if err := checkPrivileges(ctx, conn, schema, skipPub, skipSlot); err != nil {
		return err
	}

	replacer := strings.NewReplacer(
		"{schema}", schema,
		"{prefix}", prefix,
		"{publication_name}", publication,
		"{slot_name}", slot,
	)

	files, err := readSQLFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		if skipPub && strings.Contains(f.name, "create_publication") {
			fmt.Printf("skipping %s (publication creation disabled)\n", f.name)
			continue
		}
		if skipSlot && strings.Contains(f.name, "create_replication_slot") {
			fmt.Printf("skipping %s (--replica set)\n", f.name)
			continue
		}
		sql := replacer.Replace(f.content)
		fmt.Printf("executing %s...\n", f.name)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
	}

	if replica {
		if !partitionsStart.IsZero() {
			return createPartitionRange(ctx, conn, schema, prefix, partitionsStart)
		}
		return nil
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

	insertMetaSQL := fmt.Sprintf(
		"INSERT INTO %s.%s_partition_meta (partition_name, lower_bound, upper_bound) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
		schema, prefix,
	)
	if _, err := conn.Exec(ctx, insertMetaSQL, partName, now, upper); err != nil {
		return fmt.Errorf("register initial partition: %w", err)
	}

	return nil
}

func createPartitionRange(ctx context.Context, conn *pgx.Conn, schema, prefix string, start time.Time) error {
	start = start.UTC().Truncate(time.Hour)
	end := time.Now().UTC().Add(13 * time.Hour).Truncate(time.Hour)
	if !start.Before(end) {
		return fmt.Errorf("--create-partitions: start %s is not before computed end %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}

	var count int
	for t := start; t.Before(end); t = t.Add(time.Hour) {
		upper := t.Add(time.Hour)
		partName := fmt.Sprintf("%s_jobs_%s", prefix, t.Format("20060102_1504"))
		sql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s_jobs FOR VALUES FROM ('%s') TO ('%s')",
			schema, partName, schema, prefix,
			t.Format(time.RFC3339), upper.Format(time.RFC3339),
		)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("create partition %s: %w", partName, err)
		}
		count++
	}
	fmt.Printf("ensured %d partitions from %s through %s\n", count, start.Format(time.RFC3339), end.Format(time.RFC3339))
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
