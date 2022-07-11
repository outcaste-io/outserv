// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/badger/options"
	"github.com/outcaste-io/outserv/badger/table"
	"github.com/outcaste-io/outserv/badger/y"
)

// Note: If you add a new option X make sure you also add a WithX method on Options.

// Options are params for creating DB object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
//
// Each option X is documented on the WithX method.
type Options struct {
	// Required options.

	Dir string

	// Usually modified options.

	NumVersionsToKeep int
	ReadOnly          bool
	Logger            Logger
	Compression       options.CompressionType
	InMemory          bool
	MetricsEnabled    bool
	// Sets the Stream.numGo field
	NumGoroutines int

	// Fine tuning options.

	MemTableSize        int64
	BaseTableSize       int64
	BaseLevelSize       int64
	LevelSizeMultiplier int
	TableSizeMultiplier int
	MaxLevels           int

	NumMemtables int
	// Changing BlockSize across DB runs will not break badger. The block size is
	// read from the block index stored at the end of the table.
	BlockSize          int
	BloomFalsePositive float64
	BlockCacheSize     int64
	IndexCacheSize     int64

	NumLevelZeroTables      int
	NumLevelZeroTablesStall int

	NumCompactors        int
	CompactL0OnClose     bool
	LmaxCompaction       bool
	ZSTDCompressionLevel int

	// When set, checksum will be validated for each entry read from the value log file.
	VerifyValueChecksum bool

	// Encryption related options.
	EncryptionKey                 []byte        // encryption key
	EncryptionKeyRotationDuration time.Duration // key rotation duration

	// BypassLockGuard will bypass the lock guard on badger. Bypassing lock
	// guard can cause data corruption if multiple badger instances are using
	// the same directory. Use this options with caution.
	BypassLockGuard bool

	// ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
	ChecksumVerificationMode options.ChecksumVerificationMode

	// AllowStopTheWorld determines whether the DropPrefix will be blocking/non-blocking.
	AllowStopTheWorld bool

	// NamespaceOffset specifies the offset from where the next 8 bytes contains the namespace.
	NamespaceOffset int

	// Magic version used by the application using badger to ensure that it doesn't open the DB
	// with incompatible data format.
	ExternalMagicVersion uint16

	// 4. Flags for testing purposes
	// ------------------------------
	maxBatchCount int64 // max entries in batch
	maxBatchSize  int64 // max batch size in bytes

	maxValueThreshold float64
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs with the WithX methods.
func DefaultOptions(path string) Options {
	return Options{
		Dir: path,

		MemTableSize:        64 << 20,
		BaseTableSize:       2 << 20,
		BaseLevelSize:       10 << 20,
		TableSizeMultiplier: 2,
		LevelSizeMultiplier: 10,
		MaxLevels:           7,
		NumGoroutines:       8,
		MetricsEnabled:      true,
		AllowStopTheWorld:   true,

		NumCompactors:           4, // Run at least 2 compactors. Zero-th compactor prioritizes L0.
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 15,
		NumMemtables:            15,
		BloomFalsePositive:      0.01,
		BlockSize:               4 * 1024,
		NumVersionsToKeep:       1,
		CompactL0OnClose:        false,
		VerifyValueChecksum:     false,
		Compression:             options.Snappy,
		BlockCacheSize:          256 << 20,
		IndexCacheSize:          0,

		// The following benchmarks were done on a 4 KB block size (default block size). The
		// compression is ratio supposed to increase with increasing compression level but since the
		// input for compression algorithm is small (4 KB), we don't get significant benefit at
		// level 3.
		// NOTE: The benchmarks are with DataDog ZSTD that requires CGO. Hence, no longer valid.
		// no_compression-16              10	 502848865 ns/op	 165.46 MB/s	-
		// zstd_compression/level_1-16     7	 739037966 ns/op	 112.58 MB/s	2.93
		// zstd_compression/level_3-16     7	 756950250 ns/op	 109.91 MB/s	2.72
		// zstd_compression/level_15-16    1	11135686219 ns/op	   7.47 MB/s	4.38
		// Benchmark code can be found in table/builder_test.go file
		ZSTDCompressionLevel: 1,

		Logger:                        defaultLogger(INFO),
		EncryptionKey:                 []byte{},
		EncryptionKeyRotationDuration: 10 * 24 * time.Hour, // Default 10 days.
		NamespaceOffset:               -1,
	}
}

func buildTableOptions(db *DB) table.Options {
	opt := db.opt
	dk, err := db.registry.LatestDataKey()
	y.Check(err)
	return table.Options{
		ReadOnly:             opt.ReadOnly,
		MetricsEnabled:       db.opt.MetricsEnabled,
		TableSize:            uint64(opt.BaseTableSize),
		BlockSize:            opt.BlockSize,
		BloomFalsePositive:   opt.BloomFalsePositive,
		ChkMode:              opt.ChecksumVerificationMode,
		Compression:          opt.Compression,
		ZSTDCompressionLevel: opt.ZSTDCompressionLevel,
		BlockCache:           db.blockCache,
		IndexCache:           db.indexCache,
		AllocPool:            db.allocPool,
		DataKey:              dk,
	}
}

const (
	// TODO(mrjn): Increase this.
	maxValueThreshold = (1 << 20) // 1 MB
)

// parseCompression returns badger.compressionType and compression level given compression string
// of format compression-type:compression-level
func parseCompression(cStr string) (options.CompressionType, int, error) {
	cStrSplit := strings.Split(cStr, ":")
	cType := cStrSplit[0]
	level := 3

	var err error
	if len(cStrSplit) == 2 {
		level, err = strconv.Atoi(cStrSplit[1])
		y.Check(err)
		if level <= 0 {
			return 0, 0,
				errors.Errorf("ERROR: compression level(%v) must be greater than zero", level)
		}
	} else if len(cStrSplit) > 2 {
		return 0, 0, errors.Errorf("ERROR: Invalid badger.compression argument")
	}
	switch cType {
	case "zstd":
		return options.ZSTD, level, nil
	case "snappy":
		return options.Snappy, 0, nil
	case "none":
		return options.None, 0, nil
	}
	return 0, 0, errors.Errorf("ERROR: compression type (%s) invalid", cType)
}

// generateSuperFlag generates an identical SuperFlag string from the provided Options.
func generateSuperFlag(options Options) string {
	superflag := ""
	v := reflect.ValueOf(&options).Elem()
	optionsStruct := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if field := v.Field(i); field.CanInterface() {
			name := strings.ToLower(optionsStruct.Field(i).Name)
			kind := v.Field(i).Kind()
			switch kind {
			case reflect.Bool:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Bool())
			case reflect.Int, reflect.Int64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Int())
			case reflect.Uint32, reflect.Uint64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Uint())
			case reflect.Float64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Float())
			case reflect.String:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.String())
			default:
				continue
			}
		}
	}
	return superflag
}

// FromSuperFlag fills Options fields for each flag within the superflag. For
// example, replacing the default Options.NumGoroutines:
//
//	options := FromSuperFlag("numgoroutines=4", DefaultOptions(""))
//
// It's important to note that if you pass an empty Options struct, FromSuperFlag
// will not fill it with default values. FromSuperFlag only writes to the fields
// present within the superflag string (case insensitive).
//
// It specially handles compression subflag.
// Valid options are {none,snappy,zstd:<level>}
// Example: compression=zstd:3;
// Unsupported: Options.Logger, Options.EncryptionKey
func (opt Options) FromSuperFlag(superflag string) Options {
	// currentOptions act as a default value for the options superflag.
	currentOptions := generateSuperFlag(opt)
	currentOptions += "compression=;"

	flags := z.NewSuperFlag(superflag).MergeAndCheckDefault(currentOptions)
	v := reflect.ValueOf(&opt).Elem()
	optionsStruct := v.Type()
	for i := 0; i < v.NumField(); i++ {
		// only iterate over exported fields
		if field := v.Field(i); field.CanInterface() {
			// z.SuperFlag stores keys as lowercase, keep everything case
			// insensitive
			name := strings.ToLower(optionsStruct.Field(i).Name)
			if name == "compression" {
				// We will specially handle this later. Skip it here.
				continue
			}
			kind := v.Field(i).Kind()
			switch kind {
			case reflect.Bool:
				field.SetBool(flags.GetBool(name))
			case reflect.Int, reflect.Int64:
				field.SetInt(flags.GetInt64(name))
			case reflect.Uint32, reflect.Uint64:
				field.SetUint(uint64(flags.GetUint64(name)))
			case reflect.Float64:
				field.SetFloat(flags.GetFloat64(name))
			case reflect.String:
				field.SetString(flags.GetString(name))
			}
		}
	}

	// Only update the options for special flags that were present in the input superflag.
	inputFlag := z.NewSuperFlag(superflag)
	if inputFlag.Has("compression") {
		ctype, clevel, err := parseCompression(flags.GetString("compression"))
		switch {
		case err == nil:
			opt.Compression = ctype
			opt.ZSTDCompressionLevel = clevel
		default:
			ctype = options.CompressionType(flags.GetUint32("compression"))
			y.AssertTruef(ctype <= 2, "ERROR: Invalid format or compression type. Got: %s",
				flags.GetString("compression"))
			opt.Compression = ctype
		}
	}

	return opt
}

// WithDir returns a new Options value with Dir set to the given value.
//
// Dir is the path of the directory where key data will be stored in.
// If it doesn't exist, Badger will try to create it for you.
// This is set automatically to be the path given to `DefaultOptions`.
func (opt Options) WithDir(val string) Options {
	opt.Dir = val
	return opt
}

// WithNumVersionsToKeep returns a new Options value with NumVersionsToKeep set to the given value.
//
// NumVersionsToKeep sets how many versions to keep per key at most.
//
// The default value of NumVersionsToKeep is 1.
func (opt Options) WithNumVersionsToKeep(val int) Options {
	opt.NumVersionsToKeep = val
	return opt
}

// WithNumGoroutines sets the number of goroutines to be used in Stream.
//
// The default value of NumGoroutines is 8.
func (opt Options) WithNumGoroutines(val int) Options {
	opt.NumGoroutines = val
	return opt
}

// WithReadOnly returns a new Options value with ReadOnly set to the given value.
//
// When ReadOnly is true the DB will be opened on read-only mode.
// Multiple processes can open the same Badger DB.
// Note: if the DB being opened had crashed before and has vlog data to be replayed,
// ReadOnly will cause Open to fail with an appropriate message.
//
// The default value of ReadOnly is false.
func (opt Options) WithReadOnly(val bool) Options {
	opt.ReadOnly = val
	return opt
}

// WithMetricsEnabled returns a new Options value with MetricsEnabled set to the given value.
//
// When MetricsEnabled is set to false, then the DB will be opened and no badger metrics
// will be logged. Metrics are defined in metric.go file.
//
// This flag is useful for use cases like in Dgraph where we open temporary badger instances to
// index data. In those cases we don't want badger metrics to be polluted with the noise from
// those temporary instances.
//
// Default value is set to true
func (opt Options) WithMetricsEnabled(val bool) Options {
	opt.MetricsEnabled = val
	return opt
}

// WithLogger returns a new Options value with Logger set to the given value.
//
// Logger provides a way to configure what logger each value of badger.DB uses.
//
// The default value of Logger writes to stderr using the log package from the Go standard library.
func (opt Options) WithLogger(val Logger) Options {
	opt.Logger = val
	return opt
}

// WithLoggingLevel returns a new Options value with logging level of the
// default logger set to the given value.
// LoggingLevel sets the level of logging. It should be one of DEBUG, INFO,
// WARNING or ERROR levels.
//
// The default value of LoggingLevel is INFO.
func (opt Options) WithLoggingLevel(val loggingLevel) Options {
	opt.Logger = defaultLogger(val)
	return opt
}

// WithBaseTableSize returns a new Options value with MaxTableSize set to the given value.
//
// BaseTableSize sets the maximum size in bytes for LSM table or file in the base level.
//
// The default value of BaseTableSize is 2MB.
func (opt Options) WithBaseTableSize(val int64) Options {
	opt.BaseTableSize = val
	return opt
}

// WithLevelSizeMultiplier returns a new Options value with LevelSizeMultiplier set to the given
// value.
//
// LevelSizeMultiplier sets the ratio between the maximum sizes of contiguous levels in the LSM.
// Once a level grows to be larger than this ratio allowed, the compaction process will be
// triggered.
//
// The default value of LevelSizeMultiplier is 10.
func (opt Options) WithLevelSizeMultiplier(val int) Options {
	opt.LevelSizeMultiplier = val
	return opt
}

// WithMaxLevels returns a new Options value with MaxLevels set to the given value.
//
// Maximum number of levels of compaction allowed in the LSM.
//
// The default value of MaxLevels is 7.
func (opt Options) WithMaxLevels(val int) Options {
	opt.MaxLevels = val
	return opt
}

// WithNumMemtables returns a new Options value with NumMemtables set to the given value.
//
// NumMemtables sets the maximum number of tables to keep in memory before stalling.
//
// The default value of NumMemtables is 5.
func (opt Options) WithNumMemtables(val int) Options {
	opt.NumMemtables = val
	return opt
}

// WithMemTableSize returns a new Options value with MemTableSize set to the given value.
//
// MemTableSize sets the maximum size in bytes for memtable table.
//
// The default value of MemTableSize is 64MB.
func (opt Options) WithMemTableSize(val int64) Options {
	opt.MemTableSize = val
	return opt
}

// WithBloomFalsePositive returns a new Options value with BloomFalsePositive set
// to the given value.
//
// BloomFalsePositive sets the false positive probability of the bloom filter in any SSTable.
// Before reading a key from table, the bloom filter is checked for key existence.
// BloomFalsePositive might impact read performance of DB. Lower BloomFalsePositive value might
// consume more memory.
//
// The default value of BloomFalsePositive is 0.01.
//
// Setting this to 0 disables the bloom filter completely.
func (opt Options) WithBloomFalsePositive(val float64) Options {
	opt.BloomFalsePositive = val
	return opt
}

// WithBlockSize returns a new Options value with BlockSize set to the given value.
//
// BlockSize sets the size of any block in SSTable. SSTable is divided into multiple blocks
// internally. Each block is compressed using prefix diff encoding.
//
// The default value of BlockSize is 4KB.
func (opt Options) WithBlockSize(val int) Options {
	opt.BlockSize = val
	return opt
}

// WithNumLevelZeroTables sets the maximum number of Level 0 tables before compaction starts.
//
// The default value of NumLevelZeroTables is 5.
func (opt Options) WithNumLevelZeroTables(val int) Options {
	opt.NumLevelZeroTables = val
	return opt
}

// WithNumLevelZeroTablesStall sets the number of Level 0 tables that once reached causes the DB to
// stall until compaction succeeds.
//
// The default value of NumLevelZeroTablesStall is 10.
func (opt Options) WithNumLevelZeroTablesStall(val int) Options {
	opt.NumLevelZeroTablesStall = val
	return opt
}

// WithBaseLevelSize sets the maximum size target for the base level.
//
// The default value is 10MB.
func (opt Options) WithBaseLevelSize(val int64) Options {
	opt.BaseLevelSize = val
	return opt
}

// WithNumCompactors sets the number of compaction workers to run concurrently.  Setting this to
// zero stops compactions, which could eventually cause writes to block forever.
//
// The default value of NumCompactors is 2. One is dedicated just for L0 and L1.
func (opt Options) WithNumCompactors(val int) Options {
	opt.NumCompactors = val
	return opt
}

// WithCompactL0OnClose determines whether Level 0 should be compacted before closing the DB.  This
// ensures that both reads and writes are efficient when the DB is opened later.
//
// The default value of CompactL0OnClose is false.
func (opt Options) WithCompactL0OnClose(val bool) Options {
	opt.CompactL0OnClose = val
	return opt
}

// WithEncryptionKey is used to encrypt the data with AES. Type of AES is used based on the key
// size. For example 16 bytes will use AES-128. 24 bytes will use AES-192. 32 bytes will
// use AES-256.
func (opt Options) WithEncryptionKey(key []byte) Options {
	opt.EncryptionKey = key
	return opt
}

// WithEncryptionKeyRotationDuration returns new Options value with the duration set to
// the given value.
//
// Key Registry will use this duration to create new keys. If the previous generated
// key exceed the given duration. Then the key registry will create new key.
func (opt Options) WithEncryptionKeyRotationDuration(d time.Duration) Options {
	opt.EncryptionKeyRotationDuration = d
	return opt
}

// WithCompression is used to enable or disable compression. When compression is enabled, every
// block will be compressed using the specified algorithm.  This option doesn't affect existing
// tables. Only the newly created tables will be compressed.
//
// The default compression algorithm used is snappy. Compression is enabled by default.
func (opt Options) WithCompression(cType options.CompressionType) Options {
	opt.Compression = cType
	return opt
}

// WithVerifyValueChecksum is used to set VerifyValueChecksum. When VerifyValueChecksum is set to
// true, checksum will be verified for every entry read from the value log. If the value is stored
// in SST (value size less than value threshold) then the checksum validation will not be done.
//
// The default value of VerifyValueChecksum is False.
func (opt Options) WithVerifyValueChecksum(val bool) Options {
	opt.VerifyValueChecksum = val
	return opt
}

// WithChecksumVerificationMode returns a new Options value with ChecksumVerificationMode set to
// the given value.
//
// ChecksumVerificationMode indicates when the db should verify checksums for SSTable blocks.
//
// The default value of VerifyValueChecksum is options.NoVerification.
func (opt Options) WithChecksumVerificationMode(cvMode options.ChecksumVerificationMode) Options {
	opt.ChecksumVerificationMode = cvMode
	return opt
}

// WithAllowStopTheWorld returns a new Options value with AllowStopTheWorld set to the given value.
//
// AllowStopTheWorld indicates whether the call to DropPrefix should block the writes or not.
// When set to false, the DropPrefix will do a logical delete and will not block
// the writes. Although, this will not immediately clear up the LSM tree.
// When set to false, the DropPrefix will block the writes and will clear up the LSM
// tree.
//
// The default value of AllowStopTheWorld is true.
func (opt Options) WithAllowStopTheWorld(b bool) Options {
	opt.AllowStopTheWorld = b
	return opt
}

// WithBlockCacheSize returns a new Options value with BlockCacheSize set to the given value.
//
// This value specifies how much data cache should hold in memory. A small size
// of cache means lower memory consumption and lookups/iterations would take
// longer. It is recommended to use a cache if you're using compression or encryption.
// If compression and encryption both are disabled, adding a cache will lead to
// unnecessary overhead which will affect the read performance. Setting size to
// zero disables the cache altogether.
//
// Default value of BlockCacheSize is zero.
func (opt Options) WithBlockCacheSize(size int64) Options {
	opt.BlockCacheSize = size
	return opt
}

// WithInMemory returns a new Options value with Inmemory mode set to the given value.
//
// When badger is running in InMemory mode, everything is stored in memory. No value/sst files are
// created. In case of a crash all data will be lost.
func (opt Options) WithInMemory(b bool) Options {
	opt.InMemory = b
	return opt
}

// WithZSTDCompressionLevel returns a new Options value with ZSTDCompressionLevel set
// to the given value.
//
// The ZSTD compression algorithm supports 20 compression levels. The higher the compression
// level, the better is the compression ratio but lower is the performance. Lower levels
// have better performance and higher levels have better compression ratios.
// We recommend using level 1 ZSTD Compression Level. Any level higher than 1 seems to
// deteriorate badger's performance.
// The following benchmarks were done on a 4 KB block size (default block size). The compression is
// ratio supposed to increase with increasing compression level but since the input for compression
// algorithm is small (4 KB), we don't get significant benefit at level 3. It is advised to write
// your own benchmarks before choosing a compression algorithm or level.
//
// NOTE: The benchmarks are with DataDog ZSTD that requires CGO. Hence, no longer valid.
// no_compression-16              10	 502848865 ns/op	 165.46 MB/s	-
// zstd_compression/level_1-16     7	 739037966 ns/op	 112.58 MB/s	2.93
// zstd_compression/level_3-16     7	 756950250 ns/op	 109.91 MB/s	2.72
// zstd_compression/level_15-16    1	11135686219 ns/op	   7.47 MB/s	4.38
// Benchmark code can be found in table/builder_test.go file
func (opt Options) WithZSTDCompressionLevel(cLevel int) Options {
	opt.ZSTDCompressionLevel = cLevel
	return opt
}

// WithBypassLockGuard returns a new Options value with BypassLockGuard
// set to the given value.
//
// When BypassLockGuard option is set, badger will not acquire a lock on the
// directory. This could lead to data corruption if multiple badger instances
// write to the same data directory. Use this option with caution.
//
// The default value of BypassLockGuard is false.
func (opt Options) WithBypassLockGuard(b bool) Options {
	opt.BypassLockGuard = b
	return opt
}

// WithIndexCacheSize returns a new Options value with IndexCacheSize set to
// the given value.
//
// This value specifies how much memory should be used by table indices. These
// indices include the block offsets and the bloomfilters. Badger uses bloom
// filters to speed up lookups. Each table has its own bloom
// filter and each bloom filter is approximately of 5 MB.
//
// Zero value for IndexCacheSize means all the indices will be kept in
// memory and the cache is disabled.
//
// The default value of IndexCacheSize is 0 which means all indices are kept in
// memory.
func (opt Options) WithIndexCacheSize(size int64) Options {
	opt.IndexCacheSize = size
	return opt
}

// WithNamespaceOffset returns a new Options value with NamespaceOffset set to the given value. DB
// will expect the namespace in each key at the 8 bytes starting from NamespaceOffset. A negative
// value means that namespace is not stored in the key.
//
// The default value for NamespaceOffset is -1.
func (opt Options) WithNamespaceOffset(offset int) Options {
	opt.NamespaceOffset = offset
	return opt
}

// WithExternalMagic returns a new Options value with ExternalMagicVersion set to the given value.
// The DB would fail to start if either the internal or the external magic number fails validated.
func (opt Options) WithExternalMagic(magic uint16) Options {
	opt.ExternalMagicVersion = magic
	return opt
}

func (opt Options) getFileFlags() int {
	var flags int
	// opt.SyncWrites would be using msync to sync. All writes go through mmap.
	if opt.ReadOnly {
		flags |= os.O_RDONLY
	} else {
		flags |= os.O_RDWR
	}
	return flags
}
