package storm

import (
	"errors"
	"testing"
)

func TestKDFParametersAreCapped(t *testing.T) {
	tests := []struct {
		name    string
		time    uint32
		memory  uint32
		threads uint8
	}{
		{"time", maxKDFTime + 1, FastKDFMemory, 1},
		{"memory", 1, maxKDFMemory + 1, 1},
		{"threads", 1, 64 * MiB, maxKDFThreads + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKDFParams(tt.time, tt.memory, tt.threads, EncryptAll, ErrInvalidOptions)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("validateKDFParams() = %v, want ErrInvalidOptions", err)
			}
		})
	}
}
