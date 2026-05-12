package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

func BenchmarkDuckDBSink(b *testing.B) {
	dbPath := "bench_duckdb.db"
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Direct INSERT
	b.Run("Direct", func(b *testing.B) {
		setupTable(b, db, "events_direct")
		sink, _ := New(Config{DB: db, Table: "events_direct"})
		ctx := context.Background()
		payload := []byte(`{"event":"test","val":123}`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sink.WriteEvent(ctx, payload, nil)
		}
	})

	// Batched INSERT (BatchSize 100)
	b.Run("Batched_100", func(b *testing.B) {
		setupTable(b, db, "events_batched")
		sink, _ := New(Config{DB: db, Table: "events_batched", BatchSize: 100})
		ctx := context.Background()
		payload := []byte(`{"event":"test","val":123}`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sink.WriteEvent(ctx, payload, nil)
		}
		_ = sink.Flush(ctx)
	})

	// WriterLoop (BatchSize 100)
	b.Run("WriterLoop_100", func(b *testing.B) {
		setupTable(b, db, "events_loop")
		sink, _ := New(Config{DB: db, Table: "events_loop", BatchSize: 100, WriterLoop: true})
		ctx := context.Background()
		payload := []byte(`{"event":"test","val":123}`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sink.WriteEvent(ctx, payload, nil)
		}
		_ = sink.Flush(ctx)
	})

	// Appender (BatchSize 100)
	b.Run("Appender_100", func(b *testing.B) {
		setupTable(b, db, "events_appender")
		sink, _ := New(Config{DB: db, Table: "events_appender", BatchSize: 100, WriterLoop: true, UseAppender: true})
		ctx := context.Background()
		payload := []byte(`{"event":"test","val":123}`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sink.WriteEvent(ctx, payload, nil)
		}
		_ = sink.Flush(ctx)
	})
}

func setupTable(b *testing.B, db *sql.DB, table string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if err != nil {
		b.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (raw TEXT)", table))
	if err != nil {
		b.Fatal(err)
	}
}
