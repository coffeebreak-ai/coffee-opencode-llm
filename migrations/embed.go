package migrations

import "embed"

// FS holds the SQL migration files embedded into the binary.
//
//go:embed *.sql
var FS embed.FS
