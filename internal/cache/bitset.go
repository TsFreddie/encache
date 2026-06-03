package cache

import (
	"encoding/binary"
	"fmt"
)

type Bitset struct {
	bits  []byte
	size  int
	count int
}

func NewBitset(size int) *Bitset {
	return &Bitset{
		bits: make([]byte, (size+7)/8),
		size: size,
	}
}

func BitsetFromBytes(data []byte) (*Bitset, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("bitset data too short")
	}
	size := int(binary.LittleEndian.Uint32(data[:4]))
	byteCount := (size + 7) / 8
	if len(data) != 4+byteCount {
		return nil, fmt.Errorf("bitset data size mismatch")
	}

	bitset := NewBitset(size)
	copy(bitset.bits, data[4:])
	for i := 0; i < size; i++ {
		if bitset.Get(i) {
			bitset.count++
		}
	}
	return bitset, nil
}

func (b *Bitset) Size() int {
	return b.size
}

func (b *Bitset) Count() int {
	return b.count
}

func (b *Bitset) Complete() bool {
	return b.count == b.size
}

func (b *Bitset) Get(index int) bool {
	b.mustIndex(index)
	return b.bits[index/8]&(1<<uint(index%8)) != 0
}

func (b *Bitset) Set(index int, value bool) {
	b.mustIndex(index)
	mask := byte(1 << uint(index%8))
	byteIndex := index / 8
	wasSet := b.bits[byteIndex]&mask != 0

	if value {
		b.bits[byteIndex] |= mask
		if !wasSet {
			b.count++
		}
		return
	}

	b.bits[byteIndex] &^= mask
	if wasSet {
		b.count--
	}
}

func (b *Bitset) Bytes() []byte {
	data := make([]byte, 4+len(b.bits))
	binary.LittleEndian.PutUint32(data[:4], uint32(b.size))
	copy(data[4:], b.bits)
	return data
}

func (b *Bitset) mustIndex(index int) {
	if index < 0 || index >= b.size {
		panic(fmt.Sprintf("bitset index %d out of range %d", index, b.size))
	}
}
