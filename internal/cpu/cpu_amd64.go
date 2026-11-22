// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build amd64 && gc

package cpu

// cpuid and xgetbv are implemented in cpu_amd64.s.
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)
func xgetbv() (eax, edx uint32)

func init() {
	maxID, _, _, _ := cpuid(0, 0)
	_, _, ecx1, _ := cpuid(1, 0)

	X86.HasSSE41 = ecx1&(1<<19) != 0

	// AVX requires OS support for saving XMM and YMM state. OSXSAVE says the
	// OS provides XSAVE, and XGETBV reports which state components it enables.
	osxsave := ecx1&(1<<27) != 0
	avx := ecx1&(1<<28) != 0
	if !osxsave || !avx {
		return
	}
	xcr0, _ := xgetbv()
	if xcr0&0x6 != 0x6 { // XMM (bit 1) and YMM (bit 2)
		return
	}
	X86.HasAVX = true

	if maxID < 7 {
		return
	}
	_, ebx7, _, _ := cpuid(7, 0)
	X86.HasAVX2 = ebx7&(1<<5) != 0
}
