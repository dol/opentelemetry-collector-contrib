// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pdatautil // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"

// Hasher is one of two parallel serialization paths in this package; the other
// is TypedSeparatorFormatter (format.go). See format.go for an explanation of
// why the paths are separate and what must be kept in sync between them.

import (
	"encoding/binary"
	"math"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

var (
	extraByte       byte = '\xf3'
	keyPrefix       byte = '\xf4'
	valEmpty        byte = '\xf5'
	valBytesPrefix  byte = '\xf6'
	valStrPrefix    byte = '\xf7'
	valBoolTrue     byte = '\xf8'
	valBoolFalse    byte = '\xf9'
	valIntPrefix    byte = '\xfa'
	valDoublePrefix byte = '\xfb'
	valMapPrefix    byte = '\xfc'
	valMapSuffix    byte = '\xfd'
	valSlicePrefix  byte = '\xfe'
	valSliceSuffix  byte = '\xff'
	// valTerminator is used by TypedSeparatorFormatter (format.go) as a
	// universal end-of-value delimiter. It shares the same byte value as
	// valSliceSuffix but carries different semantics: in the Hasher path
	// valSliceSuffix means "end of slice", while in the formatter path
	// valTerminator means "end of any value".
	valTerminator byte = '\xff'

	emptyHash = [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

// Hasher incrementally writes the deterministic byte representation directly into
// an xxhash digest. It still buffers map keys for sorting, but it no longer
// buffers the full serialized payload before hashing.
type Hasher struct {
	digest      xxhash.Digest
	keysBuf     []string
	uint64Buf   [8]byte
	wroteData   bool
	initialized bool
}

// NewHasher creates a new streaming hasher.
func NewHasher() *Hasher {
	h := &Hasher{
		keysBuf: make([]string, 0, 16),
	}
	h.digest.Reset()
	h.initialized = true
	return h
}

var hasherPool = &sync.Pool{
	New: func() any { return NewHasher() },
}

// Reset clears the accumulated state so the hasher can be reused.
func (h *Hasher) Reset() {
	if h.keysBuf == nil {
		h.keysBuf = make([]string, 0, 16)
	} else {
		h.keysBuf = h.keysBuf[:0]
	}

	h.digest.Reset()
	h.wroteData = false
	h.initialized = true
}

// WriteMap adds a map to the hash calculation.
func (h *Hasher) WriteMap(m pcommon.Map) {
	writeMap(&h.keysBuf, h, m)
}

// WriteValue adds a value to the hash calculation.
func (h *Hasher) WriteValue(v pcommon.Value) {
	writeValue(&h.keysBuf, h, v)
}

// WriteString adds a string to the hash calculation.
func (h *Hasher) WriteString(s string) {
	writeString(h, s)
}

// WriteBool adds a boolean to the hash calculation.
func (h *Hasher) WriteBool(v bool) {
	writeBool(h, v)
}

// Sum128 returns the current hash as a [16]byte.
func (h *Hasher) Sum128() [16]byte {
	if !h.wroteData {
		return emptyHash
	}

	return h.hashSum128()
}

// Sum64 returns the current hash as a uint64.
func (h *Hasher) Sum64() uint64 {
	hash := h.Sum128()
	return xxhash.Sum64(hash[:])
}

// MapHash return a hash for the provided map.
// Maps with the same underlying key/value pairs in different order produce the same deterministic hash value.
func MapHash(m pcommon.Map) [16]byte {
	if m.Len() == 0 {
		return emptyHash
	}

	h := hasherPool.Get().(*Hasher)
	defer hasherPool.Put(h)
	h.Reset()

	h.WriteMap(m)

	return h.Sum128()
}

// ValueHash return a hash for the provided pcommon.Value.
func ValueHash(v pcommon.Value) [16]byte {
	h := hasherPool.Get().(*Hasher)
	defer hasherPool.Put(h)
	h.Reset()

	h.WriteValue(v)

	return h.Sum128()
}

func writeMap(keysBuf *[]string, w *Hasher, m pcommon.Map) {
	// For each recursive call into this function we want to preserve the previous buffer state
	// while also adding new keys to the buffer. nextIndex is the index of the first new key
	// added to the buffer for this call of the function.
	// This also works for the first non-recursive call of this function because the buffer is always empty
	// on the first call due to it being cleared of any added keys at then end of the function.
	nextIndex := len(*keysBuf)

	for k := range m.All() {
		*keysBuf = append(*keysBuf, k)
	}

	// Get only the newly added keys from the buffer by slicing the buffer from nextIndex to the end
	workingKeySet := (*keysBuf)[nextIndex:]

	sort.Strings(workingKeySet)
	for _, k := range workingKeySet {
		v, _ := m.Get(k)
		w.writeByte(keyPrefix)
		w.writeRawString(k)
		writeValue(keysBuf, w, v)
	}

	// Remove all keys that were added to the buffer during this call of the function
	*keysBuf = (*keysBuf)[:nextIndex]
}

// writeValue serialises v into w. There is no per-value terminator in this path;
// fixed-width types are self-delimiting by length, and variable-width types
// (string, bytes) are delimited by the next marker byte that follows them in
// context (keyPrefix in maps, valSliceSuffix at the end of slices).
//
// NOTE: keep this switch in sync with writeTypedSeparatorValue in format.go
// whenever a new pcommon.ValueType is introduced upstream.
func writeValue(keysBuf *[]string, w *Hasher, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		writeString(w, v.Str())
	case pcommon.ValueTypeBool:
		writeBool(w, v.Bool())
	case pcommon.ValueTypeInt:
		w.writeByte(valIntPrefix)
		w.writeUint64(uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		w.writeByte(valDoublePrefix)
		w.writeUint64(math.Float64bits(v.Double()))
	case pcommon.ValueTypeMap:
		w.writeByte(valMapPrefix)
		writeMap(keysBuf, w, v.Map())
		w.writeByte(valMapSuffix)
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		w.writeByte(valSlicePrefix)
		for i := 0; i < sl.Len(); i++ {
			writeValue(keysBuf, w, sl.At(i))
		}
		w.writeByte(valSliceSuffix)
	case pcommon.ValueTypeBytes:
		w.writeByte(valBytesPrefix)
		w.writeRawBytes(v.Bytes().AsRaw())
	case pcommon.ValueTypeEmpty:
		w.writeByte(valEmpty)
	}
}

func writeString(w *Hasher, s string) {
	w.writeByte(valStrPrefix)
	w.writeRawString(s)
}

func writeBool(w *Hasher, v bool) {
	if v {
		w.writeByte(valBoolTrue)
	} else {
		w.writeByte(valBoolFalse)
	}
}

func (h *Hasher) ensureInitialized() {
	if !h.initialized {
		h.digest.Reset()
		h.initialized = true
	}
}

func (h *Hasher) writeByte(b byte) {
	var scratch [1]byte
	scratch[0] = b
	h.writeRawBytes(scratch[:])
}

func (h *Hasher) writeUint64(v uint64) {
	binary.LittleEndian.PutUint64(h.uint64Buf[:], v)
	h.writeRawBytes(h.uint64Buf[:])
}

func (h *Hasher) writeRawString(s string) {
	h.ensureInitialized()
	h.wroteData = true
	_, _ = h.digest.WriteString(s)
}

func (h *Hasher) writeRawBytes(b []byte) {
	h.ensureInitialized()
	h.wroteData = true
	_, _ = h.digest.Write(b)
}

// hashSum128 returns a [16]byte hash sum.
func (h *Hasher) hashSum128() [16]byte {
	r := [16]byte{}
	binary.LittleEndian.PutUint64(r[:8], h.digest.Sum64())

	secondary := h.digest
	var scratch [1]byte
	scratch[0] = extraByte
	_, _ = secondary.Write(scratch[:])
	binary.LittleEndian.PutUint64(r[8:], secondary.Sum64())

	return r
}
