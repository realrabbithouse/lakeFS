package packfile

import "strings"

// PackfileAddressPrefix is the prefix used in PhysicalAddress for packfile objects.
const PackfileAddressPrefix = "packfile:"

// ParsePackfileAddress extracts the packfile ID from a packfile address.
// Returns (packfileID, true) if the address is a valid packfile address,
// or ("", false) if not.
//
// Example:
//   addr := "packfile:a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
//   packfileID, ok := ParsePackfileAddress(addr)
//   // packfileID = "a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456", ok = true
func ParsePackfileAddress(addr string) (packfileID string, ok bool) {
	if !strings.HasPrefix(addr, PackfileAddressPrefix) {
		return "", false
	}
	packfileID = strings.TrimPrefix(addr, PackfileAddressPrefix)
	if len(packfileID) != 64 {
		return "", false
	}
	return packfileID, true
}

// FormatPackfileAddress creates a packfile address string from a packfile ID.
func FormatPackfileAddress(packfileID string) string {
	return PackfileAddressPrefix + packfileID
}
