package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/buffer"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestText(t *testing.T) {

	datadriven.RunTest(t, "testdata/text", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":
			var (
				year, week uint64
			)
			td.ScanArgs(t, "year", &year)
			td.ScanArgs(t, "week", &week)
			k := rbf.View{
				Year: uint16(year),
				Week: uint8(week),
			}

			db, err := openTxt(k, txtOptions{dataPath: t.TempDir()})
			if err != nil {
				td.Fatalf(t, "failed opening text database %v", err)
				return ""
			}
			defer db.Close()
			var o bytes.Buffer
			fmt.Fprintln(&o, filepath.Base(db.db.Path()))
			var labels [][]byte
			for line := range strings.SplitSeq(td.Input, "\n") {
				if strings.HasPrefix(line, "{") {
					la, err := parser.ParseMetric(line)
					if err != nil {
						td.Fatalf(t, "failed parsing metric %v", err)
						return ""
					}
					labels = append(labels, bytes.Clone(buffer.UnwrapLabel(&la)))
					continue
				}
				if strings.HasPrefix(line, "get_tsid") {
					ids := tsidPool.Get()
					defer tsidPool.Put(ids)

					err = db.GetTSID(ids, labels)

					if err != nil {
						td.Fatalf(t, "failed assigning tsid %v", err)
						return ""
					}

					for i := range ids.B {
						fmt.Fprintln(&o, &ids.B[i])
					}
					labels = labels[:0]
				}
			}
			return o.String()
		default:
			td.Fatalf(t, "unknown command %v", td.Cmd)
			return ""
		}
	})
}
