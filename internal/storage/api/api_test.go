package api

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestPromql(t *testing.T) {
	parser.EnableExperimentalFunctions = true
	parser.EnableExtendedRangeSelectors = true
	parser.ExperimentalDurationExpr = true
	promqltest.RunBuiltinTestsWithStorage(t, newTestEngine(t), newStorage)
}

func newTestEngine(t *testing.T) *promql.Engine {
	return promqltest.NewTestEngine(t, false, 0, promqltest.DefaultMaxSamplesPerQuery)
}

func newStorage(t testutil.T) storage.Storage {
	tb := t.(testing.TB)
	tb.Helper()
	a := new(API)
	require.NoError(tb, a.Init(tb.TempDir(), nil))
	return a
}
