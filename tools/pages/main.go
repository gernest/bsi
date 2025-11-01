package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"text/tabwriter"

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

		fmt.Fprintln(os.Stdout, key.Column, key.Shard, units.BytesSize(float64(size.Bytes())))
		full += size.Bytes()
	}

	fmt.Fprintf(os.Stdout, "---\n total %v\n", units.BytesSize(float64(full)))
}

func summary(out io.Writer, key rbf.Key, size rbf.Size) {
	w := new(tabwriter.Writer)

	w.Init(out, 0, 8, 0, '\t', 0)

	for k, v := range size.Pages {
		fmt.Fprintf(w, "%d\t%d\t%s\t%d\t\n", key.Column, key.Shard, rbf.SizedPageType(k).String(), v)
	}
	w.Flush()
}
