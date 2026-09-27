// Package web embeds the demo browser client.
package web

import (
	"embed"
	"io/fs"
)

//go:embed static
var files embed.FS

// Static is the demo client rooted at the static/ directory.
func Static() fs.FS {
	sub, err := fs.Sub(files, "static")
	if err != nil {
		panic(err) // static/ is embedded at compile time
	}
	return sub
}
