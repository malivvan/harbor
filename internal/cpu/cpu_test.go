package cpu

import "testing"

func TestFeatureInvariants(t *testing.T) {
	if X86.HasAVX2 && !X86.HasAVX {
		t.Fatal("AVX2 reported without AVX")
	}
}
