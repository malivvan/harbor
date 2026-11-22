//go:build ignore

// Command main generates the binco serializer for storm.Metadata.
package main

import (
	"log"
	"os"
	"reflect"

	"github.com/malivvan/binco/codegen"
	genopts "github.com/malivvan/binco/codegen/config/gen"
)

// Metadata mirrors storm.Metadata. The generator cannot import the package
// while the generated file is missing, so keep this field list, order and types
// in sync with meta.go.
type Metadata struct {
	Salt    []byte
	Time    uint32
	Flags   uint16
	Memory  uint32
	Threads uint8
	Canary  []byte
}

func main() {
	g, err := codegen.NewGenerator(
		genopts.WithPath("github.com/malivvan/storm"),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := g.AddStruct(reflect.TypeFor[Metadata]()); err != nil {
		log.Fatal(err)
	}

	bs, err := g.Generate()
	if err != nil {
		if writeErr := os.WriteFile("failed.gen.go", bs, 0o644); writeErr != nil {
			log.Fatal(writeErr)
		}
		log.Fatal(err)
	}
	if err := os.WriteFile("meta.gen.go", bs, 0o644); err != nil {
		log.Fatal(err)
	}
}
