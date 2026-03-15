// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pdatautil // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"

// TypedSeparatorFormatter is a parallel serialization path to Hasher (hash.go).
// Both paths share the byte marker constants defined in hash.go and handle the
// same pdata value types, but they use different framing rules:
//
//   - Hasher writes raw bytes directly into an xxhash digest with no per-value
//     terminator; it uses keyPrefix before each map key and valMapSuffix/
//     valSliceSuffix as container close markers.
//
//   - TypedSeparatorFormatter accumulates bytes into a buffer and appends
//     valTerminator after every value, including container open-bracket tokens.
//
// Because the framing rules differ, the two codepaths cannot share writeValue
// or writeMap logic. If a new pcommon.ValueType is added upstream, both
// writeValue (hash.go) and writeTypedSeparatorValue (this file) must be
// updated. The parallel structure is intentional: routing needs a stable byte
// identity, not a digest, so a separate formatter avoids the digest cost.

import (
	"encoding/binary"
	"math"
	"sort"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TypedSeparatorFormatter incrementally writes a deterministic type-tagged
// sequence into an in-memory buffer using valTerminator framing. Every value,
// including container open tokens, is followed by valTerminator (\xff). This
// avoids ambiguity between consecutive variable-length values (strings, bytes)
// without needing length prefixes.
//
// The zero value is ready to use. Call Reset to reuse an instance.
type TypedSeparatorFormatter struct {
	buf       []byte
	keysBuf   []string
	uint64Buf [8]byte
}

var formatterPool = &sync.Pool{
	New: func() any { return NewTypedSeparatorFormatter() },
}

// NewTypedSeparatorFormatter creates a new typed-separator formatter with
// pre-allocated internal buffers.
func NewTypedSeparatorFormatter() *TypedSeparatorFormatter {
	return &TypedSeparatorFormatter{
		keysBuf: make([]string, 0, 16),
	}
}

// Reset clears the accumulated state so the formatter can be reused.
func (f *TypedSeparatorFormatter) Reset() {
	if f.keysBuf == nil {
		f.keysBuf = make([]string, 0, 16)
	} else {
		f.keysBuf = f.keysBuf[:0]
	}
	f.buf = f.buf[:0]
}

// WriteMap adds a map to the formatted sequence.
func (f *TypedSeparatorFormatter) WriteMap(m pcommon.Map) {
	writeTypedSeparatorMap(&f.keysBuf, f, m)
}

// WriteValue adds a value to the formatted sequence.
func (f *TypedSeparatorFormatter) WriteValue(v pcommon.Value) {
	writeTypedSeparatorValue(&f.keysBuf, f, v)
}

// WriteString adds a string to the formatted sequence.
func (f *TypedSeparatorFormatter) WriteString(v string) {
	writeTypedSeparatorString(f, v)
}

// WriteBool adds a boolean to the formatted sequence.
func (f *TypedSeparatorFormatter) WriteBool(v bool) {
	writeTypedSeparatorBool(f, v)
}

// WriteMissing marks a configured attribute as absent. The caller must
// distinguish absent attributes from attributes with an empty value; this
// writes the extraByte marker followed by valTerminator to produce a byte
// sequence that cannot collide with any typed value.
func (f *TypedSeparatorFormatter) WriteMissing() {
	f.writeByte(extraByte)
	f.writeByte(valTerminator)
}

// Bytes returns the current formatted bytes.
func (f *TypedSeparatorFormatter) Bytes() []byte {
	return f.buf
}

// String returns the current formatted sequence as a string.
func (f *TypedSeparatorFormatter) String() string {
	return string(f.buf)
}

func writeTypedSeparatorMap(keysBuf *[]string, f *TypedSeparatorFormatter, m pcommon.Map) {
	nextIndex := len(*keysBuf)
	for k := range m.All() {
		*keysBuf = append(*keysBuf, k)
	}

	workingKeySet := (*keysBuf)[nextIndex:]
	sort.Strings(workingKeySet)

	f.writeByte(valMapPrefix)
	f.writeByte(valTerminator)
	for _, k := range workingKeySet {
		writeTypedSeparatorString(f, k)
		v, _ := m.Get(k)
		writeTypedSeparatorValue(keysBuf, f, v)
	}
	f.writeByte(valTerminator)

	*keysBuf = (*keysBuf)[:nextIndex]
}

// writeTypedSeparatorValue serialises v into f. Every case appends valTerminator
// after the payload so that consecutive variable-length values (strings, bytes)
// are unambiguous without length prefixes. Container types (map, slice) write
// valTerminator after their open-bracket token and again as the close bracket,
// giving them the same self-delimiting property.
//
// NOTE: keep this switch in sync with writeValue in hash.go whenever a new
// pcommon.ValueType is introduced upstream.
func writeTypedSeparatorValue(keysBuf *[]string, f *TypedSeparatorFormatter, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		writeTypedSeparatorString(f, v.Str())
	case pcommon.ValueTypeBool:
		writeTypedSeparatorBool(f, v.Bool())
	case pcommon.ValueTypeInt:
		f.writeByte(valIntPrefix)
		f.writeUint64(uint64(v.Int()))
		f.writeByte(valTerminator)
	case pcommon.ValueTypeDouble:
		f.writeByte(valDoublePrefix)
		f.writeUint64(math.Float64bits(v.Double()))
		f.writeByte(valTerminator)
	case pcommon.ValueTypeMap:
		writeTypedSeparatorMap(keysBuf, f, v.Map())
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		f.writeByte(valSlicePrefix)
		f.writeByte(valTerminator)
		for i := 0; i < sl.Len(); i++ {
			writeTypedSeparatorValue(keysBuf, f, sl.At(i))
		}
		f.writeByte(valTerminator)
	case pcommon.ValueTypeBytes:
		f.writeByte(valBytesPrefix)
		f.writeRawBytes(v.Bytes().AsRaw())
		f.writeByte(valTerminator)
	case pcommon.ValueTypeEmpty:
		f.writeByte(valEmpty)
		f.writeByte(valTerminator)
	}
}

func writeTypedSeparatorString(f *TypedSeparatorFormatter, s string) {
	f.writeByte(valStrPrefix)
	f.writeRawString(s)
	f.writeByte(valTerminator)
}

func writeTypedSeparatorBool(f *TypedSeparatorFormatter, v bool) {
	if v {
		f.writeByte(valBoolTrue)
	} else {
		f.writeByte(valBoolFalse)
	}
	f.writeByte(valTerminator)
}

func (f *TypedSeparatorFormatter) writeByte(v byte) {
	f.buf = append(f.buf, v)
}

func (f *TypedSeparatorFormatter) writeUint64(v uint64) {
	binary.LittleEndian.PutUint64(f.uint64Buf[:], v)
	f.buf = append(f.buf, f.uint64Buf[:]...)
}

func (f *TypedSeparatorFormatter) writeRawString(v string) {
	f.buf = append(f.buf, v...)
}

func (f *TypedSeparatorFormatter) writeRawBytes(v []byte) {
	f.buf = append(f.buf, v...)
}
