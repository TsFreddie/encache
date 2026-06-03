package interceptor

import "testing"

func TestParseByteRange(t *testing.T) {
	tests := []struct {
		name   string
		header string
		size   int64
		start  int64
		end    int64
	}{
		{name: "open ended", header: "bytes=100-", size: 1000, start: 100, end: 999},
		{name: "bounded", header: "bytes=100-199", size: 1000, start: 100, end: 199},
		{name: "clamped end", header: "bytes=900-1200", size: 1000, start: 900, end: 999},
		{name: "suffix", header: "bytes=-100", size: 1000, start: 900, end: 999},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, end, err := parseByteRange(test.header, test.size)
			if err != nil {
				t.Fatalf("parse range: %v", err)
			}
			if start != test.start || end != test.end {
				t.Fatalf("range = %d-%d, want %d-%d", start, end, test.start, test.end)
			}
		})
	}
}

func TestParseByteRangeRejectsInvalid(t *testing.T) {
	for _, header := range []string{"items=0-1", "bytes=10-9", "bytes=1000-", "bytes=0-1,2-3"} {
		if _, _, err := parseByteRange(header, 1000); err == nil {
			t.Fatalf("parse %q succeeded, want error", header)
		}
	}
}
