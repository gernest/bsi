package ui

import (
	"embed"
	"net/http"

	prom_ui "github.com/prometheus/prometheus/web/ui"
)

//go:embed static
var static embed.FS

func init() {
	prom_ui.Assets = http.FS(static)
}
