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
	if err := run("binco.gen.go"); err != nil {
		log.Fatal(err)
	}
}

// run generates the serializers and writes them to path.
func run(path string) error {
	g, err := codegen.NewGenerator(
		genopts.WithPath("github.com/malivvan/storm/vec"),
	)
	if err != nil {
		return err
	}

	if err := g.AddDefinedType(reflect.TypeFor[vec.ValueKind]()); err != nil {
		return err
	}
	if err := g.AddStruct(reflect.TypeFor[vec.Value]()); err != nil {
		return err
	}
	if err := g.AddStruct(reflect.TypeFor[vec.StoredPoint]()); err != nil {
		return err
	}

	bs, err := g.Generate()
	if err != nil {
		if writeErr := os.WriteFile("failed.gen.go", bs, 0o644); writeErr != nil {
			return writeErr
		}
		return err
	}
	return os.WriteFile(path, bs, 0o644)
}
