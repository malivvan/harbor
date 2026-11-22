// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cpu provides the small subset of processor feature detection that
// the in-tree Argon2 and BLAKE2b implementations need. It replaces the
// external CPU feature-detection package for those packages.
package cpu

// X86 contains the x86/amd64 features used by the bundled hash code. Flags
// are false on non-x86 platforms or when the compiler/runtime combination does
// not provide the CPUID helpers.
var X86 struct {
	HasAVX   bool // AVX, enabled only when the OS saves XMM/YMM state
	HasAVX2  bool // AVX2, implies HasAVX
	HasSSE41 bool // SSE4.1
}

// Loong64 contains the LoongArch features used by the bundled hash code.
// LSX requires kernel vector-state support; without the auxv plumbing the
// generic path is used, which is always safe.
var Loong64 struct {
	HasLSX bool
}
