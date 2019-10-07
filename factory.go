package schemata

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

// Factory is an object which creates temporary schemas and populates them with copies of tables for testing purposes.
type Factory struct {
	pool         *sql.DB
	srcSchema    string
	prefix       string
	schemaNumber int
	mux          sync.Mutex
}

// NewFactory creates a factory object for generating test schemas.
func NewFactory(pool *sql.DB, sourceSchema string, testSchemaNamePrefix string) *Factory {
	return &Factory{
		pool:      pool,
		srcSchema: sourceSchema,
		prefix:    testSchemaNamePrefix,
	}
}


func (f *Factory) newSchemaName() string {
	f.mux.Lock()
	sn := fmt.Sprintf("%s_%d", f.prefix, f.schemaNumber)
	f.schemaNumber++
	f.mux.Unlock()
	return sn
}

// NewSchema creates a new schema, and creates tables which match the named tables in the factory's source schema.
// The returned Conn connection object will have been pre-set to the temporary schema, ready
// for testing. At the end of the tests,
func (f *Factory) NewSchema(t *testing.T, srcTableList ...string) *sql.Conn {
	t.Helper()
	conn, err := f.pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	name := f.newSchemaName()
	_, err = conn.ExecContext(context.Background(), "create schema " + name)
	if err != nil {
		conn.Close()
		t.Fatal(err)
		return nil
	}
	_, err = conn.ExecContext(context.Background(), "set schema '" + name + "'")
	if err != nil {
		conn.Close()
		t.Fatal(err)
		return nil
	}
	for _, tblname := range srcTableList {
		sql := fmt.Sprintf("create table %s.%s (like %s.%s including all)", name, tblname, f.srcSchema, tblname)
		fmt.Println(sql)
		_, err = conn.ExecContext(context.Background(), sql)
		if err != nil {
			conn.Close()
			t.Fatal(err)
			return nil
		}
	}
	return conn
}

// Close deletes the test schema and all tables and data in it, and releases the connection.
func (f *Factory) Close(t *testing.T, conn *sql.Conn) {
	t.Helper()
	res := conn.QueryRowContext(context.Background(), "select current_schema()")
	var tmpname string
	err := res.Scan(&tmpname)
	if err != nil {
		t.Fatal(err)
		return
	}
	if tmpname == f.srcSchema {
		t.Fatalf("found current schema was %s (same as source schema), expected something else", tmpname)
		return
	}
	_, err = conn.ExecContext(context.Background(), "drop schema " + tmpname + " cascade")
	if err != nil {
		t.Fatal(err)
	}
}
