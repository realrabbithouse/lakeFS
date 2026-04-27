package packfile

const (
	Magic           = "LKP1"
	Version         uint16 = 1
	HeaderSize            = 40
	ReservedBytes        = 7
	FooterSize           = 32 // SHA-256 checksum
)

const (
	ChecksumSHA256 uint8 = 0
	ChecksumBLAKE3  uint8 = 1
	ChecksumxxHash  uint8 = 2
)

const (
	CompressionNone Compression = 0
	CompressionZstd  Compression = 1
	CompressionGzip  Compression = 2
	CompressionLZ4   Compression = 3
)

const (
	ClassificationFirstClass  Classification = 0x01
	ClassificationSecondClass Classification = 0x02
)

type Compression uint8
type Checksum uint8
type Classification uint8
