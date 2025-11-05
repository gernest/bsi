package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/docker/go-units"
	"github.com/gernest/bsi/internal/rbf"
)

func main() {
	flag.Parse()
	path := flag.Arg(0)
	slog.Info("opening", "path", path)
	db := rbf.NewDB(path, nil)
	err := db.Open()
	if err != nil {
		slog.Error("opening database", "path", path, "err", err)
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		slog.Error("opening database transaction", "path", path, "err", err)
		os.Exit(1)
	}
	defer tx.Rollback()
	var full uint64
	for key, size := range tx.RangeSize() {
		if full != 0 {
			fmt.Fprintln(os.Stdout)
		}

		fmt.Fprintln(os.Stdout, key, units.BytesSize(float64(size.Bytes())))
		full += size.Bytes()
	}

	fmt.Fprintf(os.Stdout, "---\n total %v\n", units.BytesSize(float64(full)))
}
