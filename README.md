
# Schemata

[![GoDoc](https://godoc.org/github.com/lpar/schemata?status.svg)](https://godoc.org/github.com/lpar/schemata)

Helper methods for creating test schemas in PostgreSQL databases, with copies
of tables from the live schema. Thread-safe, so you can call them from unit
tests. Uses [pgx](https://github.com/jackc/pgx) because pq is sadly in
maintenance-only mode.

Needs unit tests (hint hint) but is very short, under a hundred lines.

