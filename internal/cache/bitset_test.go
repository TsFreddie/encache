package cache

import "testing"

func TestBitsetRoundTrip(t *testing.T) {
	bitset := NewBitset(17)
	bitset.Set(0, true)
	bitset.Set(8, true)
	bitset.Set(16, true)

	if bitset.Count() != 3 {
		t.Fatalf("count = %d, want 3", bitset.Count())
	}

	decoded, err := BitsetFromBytes(bitset.Bytes())
	if err != nil {
		t.Fatalf("decode bitset: %v", err)
	}
	if decoded.Size() != 17 {
		t.Fatalf("size = %d, want 17", decoded.Size())
	}
	if decoded.Count() != 3 {
		t.Fatalf("count = %d, want 3", decoded.Count())
	}
	for _, index := range []int{0, 8, 16} {
		if !decoded.Get(index) {
			t.Fatalf("bit %d is false, want true", index)
		}
	}
}

func TestBitsetSetFalseUpdatesCount(t *testing.T) {
	bitset := NewBitset(2)
	bitset.Set(1, true)
	bitset.Set(1, true)
	bitset.Set(1, false)

	if bitset.Count() != 0 {
		t.Fatalf("count = %d, want 0", bitset.Count())
	}
	if bitset.Get(1) {
		t.Fatal("bit 1 is true, want false")
	}
}

func TestBitsetDecodeRejectsSizeMismatch(t *testing.T) {
	if _, err := BitsetFromBytes([]byte{3, 0, 0, 0}); err == nil {
		t.Fatal("expected size mismatch error")
	}
}
