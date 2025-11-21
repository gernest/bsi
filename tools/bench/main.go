package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func main() {
	var models []model
	json.NewDecoder(os.Stdin).Decode(&models)

	for i := range models {
		m := &models[i]
		light := svg(m.Unit, m.Step, false, m.Entries)
		dark := svg(m.Unit, m.Step, true, m.Entries)
		os.WriteFile(fmt.Sprintf("%v-light.svg", m.Name), []byte(light), 0600)
		os.WriteFile(fmt.Sprintf("%v-dark.svg", m.Name), []byte(dark), 0600)
	}
}

type model struct {
	Name    string  `json:"name"`
	Unit    string  `json:"unit"`
	Step    float64 `json:"step"`
	Entries []entry `json:"entries"`
}

type entry struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

func svg(unit string, horizontalStep float64, dark bool, entries []entry) string {
	var mx float64
	for i := range entries {
		mx = max(mx, entries[i].Value)
	}
	topMargin := 20.0
	leftWidth := 120.0
	barHeight := 20.0
	barMargin := 3.0
	labelMargin := 8.0
	bottomHeight := 30.0
	rightWidth := 800 - leftWidth
	topHeight := float64(len(entries)) * barHeight
	width := leftWidth + rightWidth
	height := topMargin + topHeight + bottomHeight
	horizontalScale := float64((int(rightWidth) - 100) / int(mx))
	var textFill string
	if dark {
		textFill = ` fill="#C9D1D9"`
	}
	var svg []string
	svg = append(svg,
		fmt.Sprintf(`<svg width="%v" height="%v" fill="black" font-family="sans-serif" font-size="13px" xmlns="http://www.w3.org/2000/svg">`, width, height),
	)

	// Horizontal axis bars
	for i := 0.0; i*horizontalScale < rightWidth; i += horizontalStep {
		x := leftWidth + i*horizontalScale
		svg = append(svg, fmt.Sprintf(`  <rect x="%v" y="%v" width="1" height="%v" fill="#7F7F7F" fill-opacity="0.25"/>`, x, topMargin, topHeight))
	}

	// Bars
	for i := range entries {
		name, time := entries[i].Name, entries[i].Value
		y := topMargin + barHeight*float64(i)
		w := time * horizontalScale

		h := barHeight
		barY := y + barMargin
		barH := h - 2*barMargin
		var bold string
		if i == 0 {
			bold = ` font-weight="bold"`
		}
		label := name
		svg = append(svg, fmt.Sprintf(`  <rect x="%v" y="%v" width="%v" height="%v" fill="#FFCF00"/>`, leftWidth, barY, w, barH))
		svg = append(svg, fmt.Sprintf(`  <text x="%v" y="%v" text-anchor="end" dominant-baseline="middle"%v%v>%v</text>`,
			leftWidth-labelMargin, y+h/2, bold, textFill, label))
		svg = append(svg, fmt.Sprintf(`  <text x="%v" y="%v" dominant-baseline="middle"%v%v>%v%v</text>`,
			leftWidth+labelMargin+w, y+h/2, bold, textFill, time, unit))
	}

	// Horizontal labels
	for i := 0.0; i*horizontalScale < rightWidth; i += horizontalStep {
		x := leftWidth + i*horizontalScale
		y := topMargin + topHeight + labelMargin/2
		svg = append(svg, fmt.Sprintf(`  <text x="%v" y="%v" text-anchor="middle" dominant-baseline="hanging"%v>%v%v</text>`,
			x, y, textFill, i, unit))
	}
	svg = append(svg, `</svg>`)
	return strings.Join(svg, "\n")
}
