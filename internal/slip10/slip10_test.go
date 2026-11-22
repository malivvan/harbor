package slip10

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func hexMustDecode(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func TestDerive_Vector1(t *testing.T) {
	// tests according to https://github.com/satoshilabs/slips/blob/master/slip-0010.md#example
	seed := hexMustDecode("000102030405060708090a0b0c0d0e0f")

	type args struct {
		path string
		seed []byte
	}
	tests := []struct {
		name     string
		args     args
		wantPriv []byte
		wantPub  []byte

		wantErr bool
	}{
		{
			name: "Key(m) – master node",
			args: args{
				path: "m",
				seed: seed,
			},
			wantPriv: hexMustDecode("2b4be7f19ee27bbf30c667b642d5f4aa69fd169872f8fc3059c08ebae2eb19e7"),
			wantPub:  hexMustDecode("00a4b2856bfec510abab89753fac1ac0e1112364e7d250545963f135f2a33188ed"),

			wantErr: false,
		},
		{
			name: "Key(m/0')",
			args: args{
				path: "m/0'",
				seed: seed,
			},
			wantPriv: hexMustDecode("68e0fe46dfb67e368c75379acec591dad19df3cde26e63b93a8e704f1dade7a3"),
			wantPub:  hexMustDecode("008c8a13df77a28f3445213a0f432fde644acaa215fc72dcdf300d5efaa85d350c"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/1')",
			args: args{
				path: "m/0'/1'",
				seed: seed,
			},
			wantPriv: hexMustDecode("b1d0bad404bf35da785a64ca1ac54b2617211d2777696fbffaf208f746ae84f2"),
			wantPub:  hexMustDecode("001932a5270f335bed617d5b935c80aedb1a35bd9fc1e31acafd5372c30f5c1187"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/1'/2')",
			args: args{
				path: "m/0'/1'/2'",
				seed: seed,
			},
			wantPriv: hexMustDecode("92a5b23c0b8a99e37d07df3fb9966917f5d06e02ddbd909c7e184371463e9fc9"),
			wantPub:  hexMustDecode("00ae98736566d30ed0e9d2f4486a64bc95740d89c7db33f52121f8ea8f76ff0fc1"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/1'/2'/2')",
			args: args{
				path: "m/0'/1'/2'/2'",
				seed: seed,
			},
			wantPriv: hexMustDecode("30d1dc7e5fc04c31219ab25a27ae00b50f6fd66622f6e9c913253d6511d1e662"),
			wantPub:  hexMustDecode("008abae2d66361c879b900d204ad2cc4984fa2aa344dd7ddc46007329ac76c429c"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/1'/2'/2'/1000000000')",
			args: args{
				path: "m/0'/1'/2'/2'/1000000000'",
				seed: seed,
			},
			wantPriv: hexMustDecode("8f94d394a8e8fd6b1bc2f3f49f5c47e385281d5c17e65324b0f62483e37e8793"),
			wantPub:  hexMustDecode("003c24da049451555d51a7014a37337aa4e12d41e485abccfa46b47dfb2af54b7a"),

			wantErr: false,
		},
		{
			name: "Key(invalid)",
			args: args{
				path: "m/0",
				seed: seed,
			},
			wantPriv: nil,
			wantErr:  true,
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

			priv := got.PrivateKey()
			if !bytes.Equal(priv, tt.wantPriv) {
				t.Errorf("PrivateKey() = %X, want %X", priv, tt.wantPriv)
			}

			pub := got.PublicKeyWithPrefix()
			if !bytes.Equal(pub, tt.wantPub) {
				t.Errorf("PublicKeyWithPrefix() = %X, want %X", priv, tt.wantPub)
			}
		})
	}
}

func TestDerive_Vector2(t *testing.T) {
	// tests according to https://github.com/satoshilabs/slips/blob/master/slip-0010.md#example
	seed := hexMustDecode("fffcf9f6f3f0edeae7e4e1dedbd8d5d2cfccc9c6c3c0bdbab7b4b1aeaba8a5a29f9c999693908d8a8784817e7b7875726f6c696663605d5a5754514e4b484542")

	type args struct {
		path string
		seed []byte
	}
	tests := []struct {
		name     string
		args     args
		wantPriv []byte
		wantPub  []byte

		wantErr bool
	}{
		{
			name: "Key(m) – master node",
			args: args{
				path: "m",
				seed: seed,
			},
			wantPriv: hexMustDecode("171cb88b1b3c1db25add599712e36245d75bc65a1a5c9e18d76f9f2b1eab4012"),
			wantPub:  hexMustDecode("008fe9693f8fa62a4305a140b9764c5ee01e455963744fe18204b4fb948249308a"),

			wantErr: false,
		},
		{
			name: "Key(m/0')",
			args: args{
				path: "m/0'",
				seed: seed,
			},
			wantPriv: hexMustDecode("1559eb2bbec5790b0c65d8693e4d0875b1747f4970ae8b650486ed7470845635"),
			wantPub:  hexMustDecode("0086fab68dcb57aa196c77c5f264f215a112c22a912c10d123b0d03c3c28ef1037"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/2147483647')",
			args: args{
				path: "m/0'/2147483647'",
				seed: seed,
			},
			wantPriv: hexMustDecode("ea4f5bfe8694d8bb74b7b59404632fd5968b774ed545e810de9c32a4fb4192f4"),
			wantPub:  hexMustDecode("005ba3b9ac6e90e83effcd25ac4e58a1365a9e35a3d3ae5eb07b9e4d90bcf7506d"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/2147483647'/1')",
			args: args{
				path: "m/0'/2147483647'/1'",
				seed: seed,
			},
			wantPriv: hexMustDecode("3757c7577170179c7868353ada796c839135b3d30554bbb74a4b1e4a5a58505c"),
			wantPub:  hexMustDecode("002e66aa57069c86cc18249aecf5cb5a9cebbfd6fadeab056254763874a9352b45"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/2147483647'/1'/2147483646')",
			args: args{
				path: "m/0'/2147483647'/1'/2147483646'",
				seed: seed,
			},
			wantPriv: hexMustDecode("5837736c89570de861ebc173b1086da4f505d4adb387c6a1b1342d5e4ac9ec72"),
			wantPub:  hexMustDecode("00e33c0f7d81d843c572275f287498e8d408654fdf0d1e065b84e2e6f157aab09b"),

			wantErr: false,
		},
		{
			name: "Key(m/0'/2147483647'/1'/2147483646'/2')",
			args: args{
				path: "m/0'/2147483647'/1'/2147483646'/2'",
				seed: seed,
			},
			wantPriv: hexMustDecode("551d333177df541ad876a60ea71f00447931c0a9da16f227c11ea080d7391b8d"),
			wantPub:  hexMustDecode("0047150c75db263559a70d5778bf36abbab30fb061ad69f69ece61a72b0cfa4fc0"),

			wantErr: false,
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

			priv := got.PrivateKey()
			if !bytes.Equal(priv, tt.wantPriv) {
				t.Errorf("PrivateKey() = %X, want %X", priv, tt.wantPriv)
			}

			pub := got.PublicKeyWithPrefix()
			if !bytes.Equal(pub, tt.wantPub) {
				t.Errorf("PublicKeyWithPrefix() = %X, want %X", priv, tt.wantPub)
			}

		})
	}
}

func TestNew(t *testing.T) {
	seed := hexMustDecode("fffcf9f6f3f0edeae7e4e1dedbd8d5d2cfccc9c6c3c0bdbab7b4b1aeaba8a5a29f9c999693908d8a8784817e7b7875726f6c696663605d5a5754514e4b484542")

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
			want:    hexMustDecode("171cb88b1b3c1db25add599712e36245d75bc65a1a5c9e18d76f9f2b1eab4012"),
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
			if !reflect.DeepEqual(got.RawSeed(), tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMarshalUnmarshalNode(t *testing.T) {
	seed := hexMustDecode("000102030405060708090a0b0c0d0e0f")

	tests := []struct {
		name    string
		path    string
		seed    []byte
		wantErr bool
	}{
		{
			name: "master node",
			path: "m",
			seed: seed,
		},
		{
			name: "derived node m/0'",
			path: "m/0'",
			seed: seed,
		},
		{
			name: "derived node m/0'/1'/2'",
			path: "m/0'/1'/2'",
			seed: seed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create original node
			originalNode, err := Derive(tt.path, tt.seed)
			if err != nil {
				t.Fatalf("Derive() error = %v", err)
			}

			// Marshal the node
			data, err := originalNode.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			// Verify the data is 64 bytes
			if len(data) != 64 {
				t.Errorf("MarshalBinary() returned %d bytes, want 64", len(data))
			}

			// Unmarshal the node
			unmarshaledNode, err := UnmarshalNode(data)
			if err != nil {
				t.Fatalf("UnmarshalNode() error = %v", err)
			}

			// Compare the keys and public keys
			origPriv := originalNode.PrivateKey()
			unmarshaledPriv := unmarshaledNode.PrivateKey()
			if !bytes.Equal(origPriv, unmarshaledPriv) {
				t.Errorf("PrivateKey mismatch: original = %X, unmarshaled = %X", origPriv, unmarshaledPriv)
			}

			origPub := originalNode.PublicKeyWithPrefix()
			unmarshaledPub := unmarshaledNode.PublicKeyWithPrefix()
			if !bytes.Equal(origPub, unmarshaledPub) {
				t.Errorf("PublicKeyWithPrefix mismatch: original = %X, unmarshaled = %X", origPub, unmarshaledPub)
			}

			origRawSeed := originalNode.RawSeed()
			unmarshaledRawSeed := unmarshaledNode.RawSeed()
			if !bytes.Equal(origRawSeed, unmarshaledRawSeed) {
				t.Errorf("RawSeed mismatch: original = %X, unmarshaled = %X", origRawSeed, unmarshaledRawSeed)
			}
		})
	}

	// Test invalid data length
	t.Run("invalid data length", func(t *testing.T) {
		invalidData := make([]byte, 63) // Wrong size
		_, err := UnmarshalNode(invalidData)
		if err == nil {
			t.Error("UnmarshalNode() should have returned an error for invalid data length")
		}
	})
}

func TestNewRejectsInvalidSeedLength(t *testing.T) {
	for _, n := range []int{0, MinSeedSize - 1, MaxSeedSize + 1} {
		if _, err := New(make([]byte, n)); !errors.Is(err, ErrInvalidSeed) {
			t.Errorf("New(%d bytes) error = %v, want ErrInvalidSeed", n, err)
		}
	}
}

func TestDeriveRejectsOverflowingIndex(t *testing.T) {
	seed := hexMustDecode("000102030405060708090a0b0c0d0e0f")
	path := "m/2147483648'"
	if IsValidPath(path) {
		t.Fatalf("IsValidPath(%q) = true, want false", path)
	}
	if _, err := Derive(path, seed); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("Derive(%q) error = %v, want ErrInvalidPath", path, err)
	}
}

func TestRawSeedReturnsCopy(t *testing.T) {
	seed := hexMustDecode("000102030405060708090a0b0c0d0e0f")
	n, err := New(seed)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	want := hexMustDecode("2b4be7f19ee27bbf30c667b642d5f4aa69fd169872f8fc3059c08ebae2eb19e7")

	got := n.RawSeed()
	if !bytes.Equal(got, want) {
		t.Fatalf("RawSeed() = %X, want %X", got, want)
	}
	got[0] ^= 0xFF
	if again := n.RawSeed(); !bytes.Equal(again, want) {
		t.Fatalf("mutating RawSeed's result changed the node: %X", again)
	}
}

func TestNodeDestroy(t *testing.T) {
	seed := hexMustDecode("000102030405060708090a0b0c0d0e0f")
	n, err := New(seed)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	child, err := n.Derive(FirstHardenedIndex)
	if err != nil {
		t.Fatalf("Derive() error = %v", err)
	}

	n.Destroy()
	n.Destroy() // idempotent

	if pub, priv := n.KeyPair(); pub != nil || priv != nil {
		t.Error("destroyed node still returns a key pair")
	}
	if n.RawSeed() != nil {
		t.Error("destroyed node still returns a raw seed")
	}
	if n.PrivateKey() != nil {
		t.Error("destroyed node still returns a private key")
	}
	if n.PublicKeyWithPrefix() != nil {
		t.Error("destroyed node still returns a public key")
	}
	if _, err := n.MarshalBinary(); !errors.Is(err, ErrDestroyed) {
		t.Errorf("MarshalBinary() error = %v, want ErrDestroyed", err)
	}
	if _, err := n.Derive(FirstHardenedIndex); !errors.Is(err, ErrDestroyed) {
		t.Errorf("Derive() error = %v, want ErrDestroyed", err)
	}

	// Destroying a sibling node does not affect this one.
	if child.RawSeed() == nil {
		t.Error("destroying the parent wiped the child")
	}
	Destroy(child)
	if child.RawSeed() != nil {
		t.Error("package-level Destroy did not wipe the node")
	}

	var nilNode Node
	Destroy(nilNode) // must not panic
}
