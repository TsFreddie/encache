package cache

import "testing"

func TestValidateContentRange(t *testing.T) {
	if err := validateContentRange("bytes 0-99/1000", 0, 99, 1000); err != nil {
		t.Fatalf("validate content range: %v", err)
	}
}

func TestValidateContentRangeRejectsMismatch(t *testing.T) {
	if err := validateContentRange("bytes 0-98/1000", 0, 99, 1000); err == nil {
		t.Fatal("expected content range mismatch")
	}
}
