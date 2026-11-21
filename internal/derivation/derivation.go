package derivation

import (
	"fmt"

	"github.com/malivvan/storm/internal/derivation/bip39"
	"github.com/malivvan/storm/internal/derivation/slip10"
	"github.com/malivvan/storm/internal/derivation/slip21"
)

func Do() {
	slip10Node, slip21Node, err := func() (slip10.Node, slip21.Node, error) {
		seed := bip39.NewSeed(mnemonic, string(password))
		defer clear(seed)

		slip10Node, err := slip10.NewMasterNode(seed)
		if err != nil {
			return nil, nil, fmt.Errorf("error generating slip10 master node: %w", err)
		}
		slip21Node, err := slip21.NewMasterNode(seed)
		if err != nil {
			return nil, nil, fmt.Errorf("error generating slip21 master node: %w", err)
		}
		return slip10Node, slip21Node, nil
	}()
	if err != nil {
		return nil, fmt.Errorf("error generating seed: %w", err)
	}
}
