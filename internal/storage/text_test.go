package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/gernest/bsi/internal/storage/buffer"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestText(t *testing.T) {

	datadriven.RunTest(t, "testdata/text", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":

			db := new(Store)
			err := db.Init(t.TempDir(), nil)
			if err != nil {
				td.Fatalf(t, "failed opening text database %v", err)
				return ""
			}
			defer db.Close()
			var o bytes.Buffer
			fmt.Fprintln(&o, filepath.Base(db.txt.Path()))
			r := &Rows{}
			for line := range strings.SplitSeq(td.Input, "\n") {
				if strings.HasPrefix(line, "{") {
					la, err := parser.ParseMetric(line)
					if err != nil {
						td.Fatalf(t, "failed parsing metric %v", err)
						return ""
					}
					r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
					continue
				}
				if strings.HasPrefix(line, "get_tsid") {
					ids := tsidPool.Get()
					defer tsidPool.Put(ids)

					hi, err := translate(db.txt, ids, r)

					if err != nil {
						td.Fatalf(t, "failed assigning tsid %v", err)
						return ""
					}
					fmt.Fprintln(&o, hi)
					for i := range ids.B {
						fmt.Fprintln(&o, &ids.B[i])
					}
					r.Labels = r.Labels[:0]
				}
			}
			return o.String()
		default:
			td.Fatalf(t, "unknown command %v", td.Cmd)
			return ""
		}
	})
}
