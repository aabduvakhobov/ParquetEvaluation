package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "modernc.org/sqlite" // pure-Go SQLite driver: no cgo, so the binary stays static
)

const chunkSize = int64(64 * 1024 * 1024)

const writerName = "go"

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "usage: %s <input-glob> <output-parquet> <dataset-name> <sqlite-database>\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}

	inputPattern := os.Args[1]
	outputPath := os.Args[2]
	datasetName := os.Args[3]
	sqlitePath := os.Args[4]

	matches, err := expandInputPattern(inputPattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to expand input pattern: %v\n", err)
		os.Exit(1)
	}
	if len(matches) == 0 {
		fmt.Fprintf(os.Stderr, "no parquet files matched %q\n", inputPattern)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	var schema *arrow.Schema
	startTotalTime := time.Now()
	var writerOnlyTime time.Duration

	f, err := os.Create(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output file: %v\n", err)
		os.Exit(1)
	}

	writerProps := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties()
	var arrowWriter *pqarrow.FileWriter

	for _, match := range matches {
		tbl, err := readParquetTable(ctx, match)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read %s: %v\n", match, err)
			_ = f.Close()
			os.Exit(1)
		}

		if arrowWriter == nil {
			schema = tbl.Schema()
			arrowWriter, err = pqarrow.NewFileWriter(schema, f, writerProps, arrowProps)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create parquet writer: %v\n", err)
				_ = f.Close()
				os.Exit(1)
			}
		}

		if !schema.Equal(tbl.Schema()) {
			fmt.Fprintf(os.Stderr, "schema mismatch between %s and %s\n", matches[0], match)
			_ = f.Close()
			os.Exit(1)
		}
		writeStart := time.Now()
		if err := arrowWriter.WriteTable(tbl, chunkSize); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", match, err)
			_ = f.Close()
			os.Exit(1)
		}
		writerOnlyTime += time.Since(writeStart)
	}

	if arrowWriter == nil {
		fmt.Fprintln(os.Stderr, "no data available to write")
		_ = f.Close()
		os.Exit(1)
	}
	closeStart := time.Now()
	if err := arrowWriter.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close parquet writer: %v\n", err)
		_ = f.Close()
		os.Exit(1)
	}
	writerOnlyTime += time.Since(closeStart)

	totalElapsed := time.Since(startTotalTime)

	fmt.Printf("wrote %d parquet files into %s\n", len(matches), outputPath)
	fmt.Printf("Total read + write time: %.6f seconds\n", totalElapsed.Seconds())
	fmt.Printf("Writer-only time: %.6f seconds\n", writerOnlyTime.Seconds())

	// Recorded after the timers stopped, so it does not affect the measured times.
	info, err := os.Stat(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to stat output file: %v\n", err)
		os.Exit(1)
	}
	if err := recordRun(sqlitePath, datasetName, totalElapsed.Seconds(), info.Size()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to record run in %s: %v\n", sqlitePath, err)
		os.Exit(1)
	}
	fmt.Printf("Recorded run in %s (table writer_runs)\n", sqlitePath)
}

// recordRun appends one row to the writer_runs table shared by the C++, Go and Java writers,
// creating the table if it does not exist yet.
func recordRun(sqlitePath, datasetName string, totalWriteTime float64, fileSizeInBytes int64) error {
	db, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA busy_timeout = 60000"); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS writer_runs (
		dataset_name TEXT, writer TEXT, total_write_time REAL, file_size_in_bytes INTEGER)`); err != nil {
		return err
	}
	_, err = db.Exec(
		"INSERT INTO writer_runs (dataset_name, writer, total_write_time, file_size_in_bytes) VALUES (?, ?, ?, ?)",
		datasetName, writerName, totalWriteTime, fileSizeInBytes)
	return err
}

func expandInputPattern(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(matches) > 0 {
		return matches, nil
	}

	if info, err := os.Stat(pattern); err == nil && info.IsDir() {
		entries, err := os.ReadDir(pattern)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if strings.HasSuffix(strings.ToLower(entry.Name()), ".parquet") {
				matches = append(matches, filepath.Join(pattern, entry.Name()))
			}
		}
	}
	return matches, nil
}

func readParquetTable(ctx context.Context, path string) (arrow.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, err
	}

	return arrowReader.ReadTable(ctx)
}
