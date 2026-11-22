package slip21

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func hexMustDecode(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func TestDerive(t *testing.T) {
	// tests according to https://github.com/satoshilabs/slips/blob/master/slip-0021.md#example
	seed := hexMustDecode("c76c4ac4f4e4a00d6b274d5c39c700bb4a7ddc04fbc6f78e85ca75007b5b495f74a9043eeb77bdd53aa6fc3a0e31462270316fa04b8c19114c8798706cd02ac8")

	type args struct {
		path string
		seed []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Key(m) – master node",
			args: args{
				path: "m",
				seed: seed,
			},
			want:    hexMustDecode("dbf12b44133eaab506a740f6565cc117228cbf1dd70635cfa8ddfdc9af734756"),
			wantErr: false,
		},
		{
			name: "Key(m/SLIP-0021)",
			args: args{
				path: "m/SLIP-0021",
				seed: seed,
			},
			want:    hexMustDecode("1d065e3ac1bbe5c7fad32cf2305f7d709dc070d672044a19e610c77cdf33de0d"),
			wantErr: false,
		},
		{
			name: "Key(m/SLIP-0021/Master encryption key)",
			args: args{
				path: "m/SLIP-0021/Master encryption key",
				seed: seed,
			},
			want:    hexMustDecode("ea163130e35bbafdf5ddee97a17b39cef2be4b4f390180d65b54cf05c6a82fde"),
			wantErr: false,
		},
		{
			name: "Key(m/SLIP-0021/Authentication key)",
			args: args{
				path: "m/SLIP-0021/Authentication key",
				seed: seed,
			},
			want:    hexMustDecode("47194e938ab24cc82bfa25f6486ed54bebe79c40ae2a5a32ea6db294d81861a6"),
			wantErr: false,
		},
		{
			name: "Key(invalid)",
			args: args{
				path: "invalid",
				seed: seed,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Derive(tt.args.path, tt.args.seed)
			if (err != nil) != tt.wantErr {
				t.Errorf("Derive() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(got.Key(), tt.want) {
				t.Errorf("Derive() = %X, want %X", got.Key(), tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	seed := hexMustDecode("c76c4ac4f4e4a00d6b274d5c39c700bb4a7ddc04fbc6f78e85ca75007b5b495f74a9043eeb77bdd53aa6fc3a0e31462270316fa04b8c19114c8798706cd02ac8")

	type args struct {
		seed []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "get a master key",
			args: args{
				seed: seed,
			},
			want:    hexMustDecode("dbf12b44133eaab506a740f6565cc117228cbf1dd70635cfa8ddfdc9af734756"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.seed)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Key(), tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewRejectsEmptySeed(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrInvalidSeed) {
		t.Errorf("New(nil) error = %v, want ErrInvalidSeed", err)
	}
	// SLIP-0021 does not bound the seed length; non-empty seeds are accepted.
	if _, err := New(make([]byte, 128)); err != nil {
		t.Errorf("New(128 bytes) error = %v, want nil", err)
	}
}

func TestDeriveRejectsEmptyLabel(t *testing.T) {
	seed := hexMustDecode("c76c4ac4f4e4a00d6b274d5c39c700bb4a7ddc04fbc6f78e85ca75007b5b495f74a9043eeb77bdd53aa6fc3a0e31462270316fa04b8c19114c8798706cd02ac8")
	for _, path := range []string{"m//key", "m/SLIP-0021/"} {
		if IsValidPath(path) {
			t.Errorf("IsValidPath(%q) = true, want false", path)
		}
		if _, err := Derive(path, seed); !errors.Is(err, ErrInvalidPath) {
			t.Errorf("Derive(%q) error = %v, want ErrInvalidPath", path, err)
		}
	}
}

func TestKeyReturnsCopy(t *testing.T) {
	seed := hexMustDecode("c76c4ac4f4e4a00d6b274d5c39c700bb4a7ddc04fbc6f78e85ca75007b5b495f74a9043eeb77bdd53aa6fc3a0e31462270316fa04b8c19114c8798706cd02ac8")
	n, err := New(seed)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	want := hexMustDecode("dbf12b44133eaab506a740f6565cc117228cbf1dd70635cfa8ddfdc9af734756")

	got := n.Key()
	if !bytes.Equal(got, want) {
		t.Fatalf("Key() = %X, want %X", got, want)
	}
	got[0] ^= 0xFF
	if again := n.Key(); !bytes.Equal(again, want) {
		t.Fatalf("mutating Key's result changed the node: %X", again)
	}
}

func TestNodeDestroy(t *testing.T) {
	seed := hexMustDecode("c76c4ac4f4e4a00d6b274d5c39c700bb4a7ddc04fbc6f78e85ca75007b5b495f74a9043eeb77bdd53aa6fc3a0e31462270316fa04b8c19114c8798706cd02ac8")
	n, err := New(seed)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	child, err := n.Derive([]byte(Prefix))
	if err != nil {
		t.Fatalf("Derive() error = %v", err)
	}

	n.Destroy()
	n.Destroy() // idempotent

	if n.Key() != nil {
		t.Error("destroyed node still returns a key")
	}
	if _, err := n.Derive([]byte("label")); !errors.Is(err, ErrDestroyed) {
		t.Errorf("Derive() error = %v, want ErrDestroyed", err)
	}

	// Destroying the parent does not affect an already derived child.
	if child.Key() == nil {
		t.Error("destroying the parent wiped the child")
	}
	Destroy(child)
	if child.Key() != nil {
		t.Error("package-level Destroy did not wipe the node")
	}

	var nilNode Node
	Destroy(nilNode) // must not panic
}

func TestDeriveErrorBranches(t *testing.T) {
	_, err := Derive("not-a-path", []byte("seed"))
	require.ErrorIs(t, err, ErrInvalidPath)

	_, err = Derive(Prefix+"/key", nil)
	require.ErrorIs(t, err, ErrInvalidSeed)

	_, err = New(nil)
	require.ErrorIs(t, err, ErrInvalidSeed)

	derived, err := Derive(Prefix+"/key", []byte("seed"))
	require.NoError(t, err)
	require.NotEmpty(t, derived.Key())

	derived.Destroy()
	require.Empty(t, derived.Key())
	_, err = derived.Derive([]byte("child"))
	require.ErrorIs(t, err, ErrDestroyed)

	var nilNode *node
	_, err = nilNode.Derive([]byte("child"))
	require.ErrorIs(t, err, ErrDestroyed)
}
