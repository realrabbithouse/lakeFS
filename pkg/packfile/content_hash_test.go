package packfile

import (
	"strings"
	"testing"
)

func TestContentHashString(t *testing.T) {
	tests := []struct {
		name     string
		hash     ContentHash
		expected string
	}{
		{
			name:     "zero value returns 64 zeros",
			hash:     ContentHash{},
			expected: "0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:     "non-zero value returns hex string",
			hash:     ContentHash([32]byte{0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12}),
			expected: "abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12",
		},
		{
			name:     "all 0xff returns 64 f's",
			hash:     ContentHash([32]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
			expected: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			name:     "01020304...1f20 pattern",
			hash:     ContentHash([32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}),
			expected: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.hash.String()
			if got != tt.expected {
				t.Errorf("ContentHash.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseContentHash(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantHash  ContentHash
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "valid 64 char lowercase hex",
			input:    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			wantHash: ContentHash([32]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}),
			wantErr:  false,
		},
		{
			name:     "valid 64 char uppercase hex",
			input:    "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
			wantHash: ContentHash([32]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}),
			wantErr:  false,
		},
		{
			name:     "valid 64 char mixed case hex",
			input:    "abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12",
			wantHash: ContentHash([32]byte{0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12}),
			wantErr:  false,
		},
		{
			name:      "too short - 63 chars",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1",
			wantHash:  ContentHash{},
			wantErr:   true,
			errSubstr: "invalid length",
		},
		{
			name:      "too long - 65 chars",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021",
			wantHash:  ContentHash{},
			wantErr:   true,
			errSubstr: "invalid length",
		},
		{
			name:      "invalid char - g in string",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1g",
			wantHash:  ContentHash{},
			wantErr:   true,
			errSubstr: "invalid hex",
		},
		{
			name:      "empty string",
			input:     "",
			wantHash:  ContentHash{},
			wantErr:   true,
			errSubstr: "invalid length",
		},
		{
			name:      "only spaces",
			input:     "                                                                ",
			wantHash:  ContentHash{},
			wantErr:   true,
			errSubstr: "invalid hex",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseContentHash(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseContentHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("ParseContentHash() error = %v, want error containing %q", err, tt.errSubstr)
				return
			}
			if !tt.wantErr && got != tt.wantHash {
				t.Errorf("ParseContentHash() = %v, want %v", got, tt.wantHash)
			}
		})
	}
}

func TestContentHashFromBytes(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    ContentHash
		wantErr bool
	}{
		{
			name:    "valid 32 bytes",
			input:   []byte{0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12},
			want:    ContentHash([32]byte{0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12}),
			wantErr: false,
		},
		{
			name:    "empty slice",
			input:   []byte{},
			want:    ContentHash{},
			wantErr: true,
		},
		{
			name:    "29 bytes - too short",
			input:   make([]byte, 29),
			want:    ContentHash{},
			wantErr: true,
		},
		{
			name:    "33 bytes - too long",
			input:   make([]byte, 33),
			want:    ContentHash{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ContentHashFromBytes(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ContentHashFromBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ContentHashFromBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContentHashBytesRoundtrip(t *testing.T) {
	original := ContentHash([32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20})
	hexStr := original.String()
	parsed, err := ParseContentHash(hexStr)
	if err != nil {
		t.Fatalf("ParseContentHash() failed: %v", err)
	}
	if parsed != original {
		t.Errorf("ParseContentHash roundtrip: got %v, want %v", parsed, original)
	}
}

func TestContentHashFromBytesToHex(t *testing.T) {
	rawBytes := make([]byte, 32)
	for i := range rawBytes {
		rawBytes[i] = byte(i + 1)
	}
	ch, err := ContentHashFromBytes(rawBytes)
	if err != nil {
		t.Fatalf("ContentHashFromBytes() failed: %v", err)
	}
	expectedHex := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	if got := ch.String(); got != expectedHex {
		t.Errorf("ContentHash.String() = %q, want %q", got, expectedHex)
	}
}

func TestContentHashNoPrefix(t *testing.T) {
	ch, err := ParseContentHash("0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatalf("ParseContentHash() failed: %v", err)
	}
	s := ch.String()
	if strings.HasPrefix(s, "sha256:") {
		t.Errorf("ContentHash.String() should not have sha256: prefix, got %q", s)
	}
	if strings.Contains(s, ":") {
		t.Errorf("ContentHash.String() should not contain colon, got %q", s)
	}
}

func TestPackfileIDString(t *testing.T) {
	tests := []struct {
		name     string
		id       PackfileID
		expected string
	}{
		{
			name:     "valid packfile id",
			id:       PackfileID("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
			expected: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		},
		{
			name:     "another valid id",
			id:       PackfileID("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
			expected: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.id)
			if got != tt.expected {
				t.Errorf("PackfileID string = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParsePackfileID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantID    PackfileID
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "valid 64 char lowercase hex",
			input:    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			wantID:   PackfileID("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
			wantErr:  false,
		},
		{
			name:     "valid 64 char uppercase hex",
			input:    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
			wantID:   PackfileID("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
			wantErr:  false,
		},
		{
			name:      "too short - 63 chars",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1",
			wantID:    PackfileID(""),
			wantErr:   true,
			errSubstr: "invalid length",
		},
		{
			name:      "too long - 65 chars",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			wantID:    PackfileID(""),
			wantErr:   true,
			errSubstr: "invalid length",
		},
		{
			name:      "invalid char - g in string",
			input:     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1g",
			wantID:    PackfileID(""),
			wantErr:   true,
			errSubstr: "invalid hex",
		},
		{
			name:      "empty string",
			input:     "",
			wantID:    PackfileID(""),
			wantErr:   true,
			errSubstr: "invalid length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePackfileID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePackfileID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("ParsePackfileID() error = %v, want error containing %q", err, tt.errSubstr)
				return
			}
			if !tt.wantErr && got != tt.wantID {
				t.Errorf("ParsePackfileID() = %v, want %v", got, tt.wantID)
			}
		})
	}
}

func TestParseUploadID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantID    UploadID
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "valid ULID - 26 uppercase chars",
			input:    "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			wantID:   UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
			wantErr:  false,
		},
		{
			name:     "another valid ULID",
			input:    "AAAAAAAAAAAAAAAAAAAAAAAAAA",
			wantID:   UploadID("AAAAAAAAAAAAAAAAAAAAAAAAAA"),
			wantErr:  false,
		},
		{
			name:      "too short - 25 chars",
			input:     "01ARZ3NDEKTSV4RRFFQ69G5FA",
			wantID:    UploadID(""),
			wantErr:   true,
			errSubstr: "must be 26 characters",
		},
		{
			name:      "too long - 27 chars",
			input:     "01ARZ3NDEKTSV4RRFFQ69G5FAVA",
			wantID:    UploadID(""),
			wantErr:   true,
			errSubstr: "must be 26 characters",
		},
		{
			name:      "lowercase letters accepted and normalized to uppercase",
			input:    "01arz3ndektsv4rrffq69g5fav",
			wantID:   UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
			wantErr:  false,
		},
		{
			name:      "empty string",
			input:     "",
			wantID:    UploadID(""),
			wantErr:   true,
			errSubstr: "must be 26 characters",
		},
		{
			name:      "invalid char - space",
			input:    "01ARZ3NDEKTSV4RRFFQ69G5FA ",
			wantID:    UploadID(""),
			wantErr:   true,
			errSubstr: "invalid ULID character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUploadID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUploadID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("ParseUploadID() error = %v, want error containing %q", err, tt.errSubstr)
				return
			}
			if !tt.wantErr && got != tt.wantID {
				t.Errorf("ParseUploadID() = %v, want %v", got, tt.wantID)
			}
		})
	}
}

func TestUploadIDString(t *testing.T) {
	id := UploadID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if got := string(id); got != "01ARZ3NDEKTSV4RRFFQ69G5FAV" {
		t.Errorf("UploadID string = %q, want %q", got, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	}
}

func BenchmarkContentHashString(b *testing.B) {
	ch := ContentHash([32]byte{0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12, 0xab, 0xcd, 0xef, 0x12})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ch.String()
	}
}

func BenchmarkParseContentHash(b *testing.B) {
	input := "0000000000000000000000000000000000000000000000000000000000000000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseContentHash(input)
	}
}

func BenchmarkParsePackfileID(b *testing.B) {
	input := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParsePackfileID(input)
	}
}

func BenchmarkParseUploadID(b *testing.B) {
	input := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseUploadID(input)
	}
}