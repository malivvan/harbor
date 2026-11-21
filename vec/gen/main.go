// Command gen writes the binco serializers for the vec package.
package main

import (
	"log"
	"os"
	"reflect"

	"github.com/malivvan/binco/codegen"
	genopts "github.com/malivvan/binco/codegen/config/gen"
	"github.com/malivvan/storm/vec"
)

func main() {
	g, err := codegen.NewGenerator(
		genopts.WithPath("github.com/malivvan/storm/vec"),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := g.AddDefinedType(reflect.TypeFor[vec.ValueKind]()); err != nil {
		log.Fatal(err)
	}
	if err := g.AddStruct(reflect.TypeFor[vec.Value]()); err != nil {
		log.Fatal(err)
	}
	if err := g.AddStruct(reflect.TypeFor[vec.StoredPoint]()); err != nil {
		log.Fatal(err)
	}

	bs, err := g.Generate()
	if err != nil {
		if writeErr := os.WriteFile("failed.gen.go", bs, 0o644); writeErr != nil {
			log.Fatal(writeErr)
		}
		log.Fatal(err)
	}
	if err := os.WriteFile("binco.gen.go", bs, 0o644); err != nil {
		log.Fatal(err)
	}
}
