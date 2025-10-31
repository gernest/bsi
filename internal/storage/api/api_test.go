package api

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
)

const in = `load 5m
  http_requests{job="api-server", instance="0", group="production"} 0+10x10
  http_requests{job="api-server", instance="1", group="production"} 0+20x10
  http_requests{job="api-server", instance="0", group="canary"}   0+30x10
  http_requests{job="api-server", instance="1", group="canary"}   0+40x10
  http_requests{job="app-server", instance="0", group="production"} 0+50x10
  http_requests{job="app-server", instance="1", group="production"} 0+60x10
  http_requests{job="app-server", instance="0", group="canary"}   0+70x10
  http_requests{job="app-server", instance="1", group="canary"}   0+80x10

load 5m
  foo{job="api-server", instance="0", region="europe"} 0+90x10
  foo{job="api-server"} 0+100x10

# Simple sum.
eval instant at 50m SUM BY (group) (http_requests{job="api-server"})
  {group="canary"} 700
  {group="production"} 300
`

func TestPromql(t *testing.T) {
	promqltest.RunTestWithStorage(t, in, newTestEngine(t), newStorage)
}

func newTestEngine(t *testing.T) *promql.Engine {
	return promqltest.NewTestEngine(t, false, 0, promqltest.DefaultMaxSamplesPerQuery)
}

func newStorage(t testutil.T) storage.Storage {
	tb := t.(testing.TB)
	tb.Helper()
	a := new(API)
	require.NoError(tb, a.Init(tb.TempDir()))
	return a
}
