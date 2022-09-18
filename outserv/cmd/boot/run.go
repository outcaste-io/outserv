// Portions Copyright 2017-2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package boot

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof" // http profiler
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/outcaste-io/outserv/badger"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/chunker"
	"github.com/outcaste-io/outserv/ee"
	"github.com/outcaste-io/outserv/filestore"
	gqlSchema "github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/xidmap"
	"github.com/outcaste-io/ristretto/z"

	"github.com/outcaste-io/outserv/tok"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
)

// Boot is the sub-command invoked when running "dgraph boot".
var Boot x.SubCommand

var defaultOutDir = "./out"

const BadgerDefaults = "compression=snappy; numgoroutines=8;"

func init() {
	Boot.Cmd = &cobra.Command{
		Use:   "boot",
		Short: "Run Outserv Boot Loader",
		Run: func(cmd *cobra.Command, args []string) {
			defer x.StartProfile(Boot.Conf).Stop()
			run()
		},
		Annotations: map[string]string{"group": "data-load"},
	}
	Boot.Cmd.SetHelpTemplate(x.NonRootTemplate)
	Boot.EnvPrefix = "OUTSERV_BOOT"

	flag := Boot.Cmd.Flags()
	flag.StringP("ipc", "i", "", "Directory or path of IPC files.")
	flag.StringP("files", "f", "",
		"Location of *.json(.gz) file(s) to load.")
	flag.StringP("schema", "s", "", "Location of the GraphQL schema file.")
	flag.StringP("type", "t", "", "GraphQL type of the root object in file."+
		" Useful if no @type field is present in the JSON objects.")
	flag.String("out", defaultOutDir,
		"Location to write the final dgraph data directories.")
	flag.Bool("replace_out", false,
		"Replace out directory and its contents if it exists.")
	flag.String("map", "map",
		"Map directory used to use for on-disk scratch space. Requires free space proportional"+
			" to the size of the RDF file and the amount of indexing used.")
	flag.String("buf", "buf",
		"Buffer dir used for temporary buffers. Ideally located in a fast drive.")

	flag.IntP("num_go_routines", "j", int(math.Ceil(float64(runtime.NumCPU())*0.85)),
		"Number of worker threads to use. MORE THREADS LEAD TO HIGHER RAM USAGE.")
	flag.Int64("mapoutput_mb", 4096,
		"The estimated size of each map file output. Increasing this increases memory usage.")
	flag.Int64("partition_mb", 8, "Pick a partition key every N megabytes of data.")
	flag.Bool("skip_map_phase", false,
		"Skip the map phase (assumes that map output files already exist).")
	flag.Bool("cleanup_map", false,
		"Clean up the map directory after the loader finishes. Setting this to false allows the"+
			" boot loader to be re-run while skipping the map phase.")
	flag.Int("reducers", 1,
		"Number of reducers to run concurrently. Increasing this can improve performance, and "+
			"must be less than or equal to the number of reduce shards.")
	flag.Bool("version", false, "Prints the version of Dgraph Boot Loader.")
	flag.String("unique_types", "", "List of types which only have one instance."+
		" We can skip the XID map for these.")
	flag.String("xidmap", "", "Directory to store xid to uid mapping")
	// TODO: Potentially move http server to main.
	flag.String("http", "localhost:8080", "Address to serve http (pprof).")
	flag.Bool("ignore_errors", false, "ignore parsing errors in JSON files")
	flag.Int("map_shards", 1,
		"Number of map output shards. Must be greater than or equal to the number of reduce "+
			"shards. Increasing allows more evenly sized reduce shards, at the expense of "+
			"increased memory usage.")
	flag.Int("reduce_shards", 1,
		"Number of reduce shards. This determines the number of dgraph instances in the final "+
			"cluster. Increasing this potentially decreases the reduce stage runtime by using "+
			"more parallelism, but increases memory usage.")
	flag.String("custom_tokenizers", "",
		"Comma separated list of tokenizer plugins")
	flag.Uint64("force-namespace", 0,
		"Namespace onto which to load the data. If not set, will preserve the namespace."+
			" When using this flag to load data into specific namespace, make sure that the "+
			"load data do not have ACL data.")
	flag.Int64("max-splits", 1000,
		"How many splits can a single key have, before it is forbidden. Also known as Jupiter key.")

	flag.String("badger", BadgerDefaults, z.NewSuperFlagHelp(BadgerDefaults).
		Head("Badger options (Refer to badger documentation for all possible options)").
		Flag("compression",
			"Specifies the compression algorithm and compression level (if applicable) for the "+
				`postings directory. "none" would disable compression, while "zstd:1" would set `+
				"zstd compression at level 1.").
		Flag("numgoroutines",
			"The number of goroutines to use in badger.Stream.").
		String())
}

func run() {
	cacheSize := 64 << 20 // These are the default values. User can overwrite them using --badger.
	cacheDefaults := fmt.Sprintf("indexcachesize=%d; blockcachesize=%d; ",
		(70*cacheSize)/100, (30*cacheSize)/100)

	bopts := badger.DefaultOptions("").FromSuperFlag(BadgerDefaults + cacheDefaults).
		FromSuperFlag(Boot.Conf.GetString("badger"))
	keys, err := ee.GetKeys(Boot.Conf)
	x.Check(err)

	opt := options{
		DataFiles:        Boot.Conf.GetString("files"),
		DataIPC:          Boot.Conf.GetString("ipc"),
		EncryptionKey:    keys.EncKey,
		GqlSchemaFile:    Boot.Conf.GetString("schema"),
		GqlType:          Boot.Conf.GetString("type"),
		OutDir:           Boot.Conf.GetString("out"),
		ReplaceOutDir:    Boot.Conf.GetBool("replace_out"),
		MapDir:           Boot.Conf.GetString("map"),
		BufDir:           Boot.Conf.GetString("buf"),
		NumGoroutines:    Boot.Conf.GetInt("num_go_routines"),
		MapBufSize:       uint64(Boot.Conf.GetInt("mapoutput_mb")),
		PartitionBufSize: int64(Boot.Conf.GetInt("partition_mb")),
		SkipMapPhase:     Boot.Conf.GetBool("skip_map_phase"),
		CleanupTmp:       Boot.Conf.GetBool("cleanup_tmp"),
		NumReducers:      Boot.Conf.GetInt("reducers"),
		Version:          Boot.Conf.GetBool("version"),
		ZeroAddr:         Boot.Conf.GetString("zero"),
		HttpAddr:         Boot.Conf.GetString("http"),
		IgnoreErrors:     Boot.Conf.GetBool("ignore_errors"),
		MapShards:        Boot.Conf.GetInt("map_shards"),
		ReduceShards:     Boot.Conf.GetInt("reduce_shards"),
		CustomTokenizers: Boot.Conf.GetString("custom_tokenizers"),
		ClientDir:        Boot.Conf.GetString("xidmap"),
		Namespace:        Boot.Conf.GetUint64("force-namespace"),
		Badger:           bopts,
	}

	opt.UniqueTypes = make(map[string]bool)
	if typeStr := Boot.Conf.GetString("unique_types"); len(typeStr) > 0 {
		types := strings.Split(typeStr, ",")
		for _, typ := range types {
			opt.UniqueTypes[typ] = true
		}
	}

	// set MaxSplits because while boot-loading alpha won't be running and rollup would not be
	// able to pick value for max-splits from x.Config.Limit.
	posting.MaxSplits = Boot.Conf.GetInt("max-splits")

	x.PrintVersion()
	if opt.Version {
		os.Exit(0)
	}

	if !filestore.Exists(opt.GqlSchemaFile) {
		fmt.Fprintf(os.Stderr, "Schema path(%v) does not exist.\n", opt.GqlSchemaFile)
		os.Exit(1)
	}

	if len(opt.DataFiles) > 0 {
		fileList := strings.Split(opt.DataFiles, ",")
		for _, file := range fileList {
			if !filestore.Exists(file) {
				fmt.Fprintf(os.Stderr, "Data path(%v) does not exist.\n", file)
				os.Exit(1)
			}
		}
	}

	if opt.ReduceShards > opt.MapShards {
		fmt.Fprintf(os.Stderr, "Invalid flags: reduce_shards(%d) should be <= map_shards(%d)\n",
			opt.ReduceShards, opt.MapShards)
		os.Exit(1)
	}
	if opt.NumReducers > opt.ReduceShards {
		fmt.Fprintf(os.Stderr, "Invalid flags: shufflers(%d) should be <= reduce_shards(%d)\n",
			opt.NumReducers, opt.ReduceShards)
		os.Exit(1)
	}
	if opt.CustomTokenizers != "" {
		for _, soFile := range strings.Split(opt.CustomTokenizers, ",") {
			tok.LoadCustomTokenizer(soFile)
		}
	}
	if opt.MapBufSize <= 0 || opt.PartitionBufSize <= 0 {
		fmt.Fprintf(os.Stderr, "mapoutput_mb: %d and partition_mb: %d must be greater than zero\n",
			opt.MapBufSize, opt.PartitionBufSize)
		os.Exit(1)
	}

	opt.MapBufSize <<= 20       // Convert from MB to B.
	opt.PartitionBufSize <<= 20 // Convert from MB to B.

	optBuf, err := json.MarshalIndent(&opt, "", "\t")
	x.Check(err)
	fmt.Println(string(optBuf))

	maxOpenFilesWarning()

	go func() {
		log.Fatal(http.ListenAndServe(opt.HttpAddr, nil))
	}()
	http.HandleFunc("/jemalloc", x.JemallocHandler)

	// Make sure it's OK to create or replace the directory specified with the --out option.
	// It is always OK to create or replace the default output directory.
	if opt.OutDir != defaultOutDir && !opt.ReplaceOutDir {
		err := x.IsMissingOrEmptyDir(opt.OutDir)
		if err == nil {
			fmt.Fprintf(os.Stderr, "Output directory exists and is not empty."+
				" Use --replace_out to overwrite it.\n")
			os.Exit(1)
		} else if err != x.ErrMissingDir {
			x.CheckfNoTrace(err)
		}
	}

	// Delete and recreate the output dirs to ensure they are empty.
	x.Check(os.RemoveAll(opt.OutDir))
	for i := 0; i < opt.ReduceShards; i++ {
		dir := filepath.Join(opt.OutDir, strconv.Itoa(i), "p")
		x.Check(os.MkdirAll(dir, 0700))
		opt.shardOutputDirs = append(opt.shardOutputDirs, dir)

		x.Check(x.WriteGroupIdFile(dir, uint32(i+1)))
	}

	// Create a directory just for boot loader's usage.
	if !opt.SkipMapPhase {
		x.Check(os.RemoveAll(opt.MapDir))
		x.Check(os.MkdirAll(opt.MapDir, 0700))
	}
	if opt.CleanupTmp {
		defer os.RemoveAll(opt.MapDir)
	}

	// Create directory for temporary buffers used in map-reduce phase
	bufDir := opt.BufDir
	x.Check(os.RemoveAll(bufDir))
	x.Check(os.MkdirAll(bufDir, 0700))
	defer os.RemoveAll(bufDir)

	loader := newLoader(&opt)

	const bootMetaFilename = "boot.meta"
	bootMetaPath := filepath.Join(opt.MapDir, bootMetaFilename)

	if opt.SkipMapPhase {
		bootMetaData, err := ioutil.ReadFile(bootMetaPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading from bulk meta file")
			os.Exit(1)
		}

		var bootMeta pb.BulkMeta
		if err = bootMeta.Unmarshal(bootMetaData); err != nil {
			fmt.Fprintln(os.Stderr, "Error deserializing bulk meta file")
			os.Exit(1)
		}

		loader.prog.mapEdgeCount = bootMeta.EdgeCount
		loader.dqlSchema.schemaMap = bootMeta.SchemaMap
	} else {
		loader.mapStage()
		mergeMapShardsIntoReduceShards(&opt)

		bootMeta := pb.BulkMeta{
			EdgeCount: loader.prog.mapEdgeCount,
			SchemaMap: loader.dqlSchema.schemaMap,
		}
		bootMetaData, err := bootMeta.Marshal()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error serializing bulk meta file")
			os.Exit(1)
		}
		if err = ioutil.WriteFile(bootMetaPath, bootMetaData, 0644); err != nil {
			fmt.Fprintln(os.Stderr, "Error writing to bulk meta file")
			os.Exit(1)
		}
		maxUid := loader.xids.AllocateUid()
		for i := 0; i < opt.ReduceShards; i++ {
			dir := filepath.Join(opt.OutDir, strconv.Itoa(i), "p")
			x.Check(writeUIDFile(dir, maxUid))
		}
	}

	loader.reduceStage()
	loader.writeSchema()
	loader.cleanup()
}

func writeUIDFile(pdir string, maxUid uint64) error {
	uidFile := filepath.Join(pdir, "max_uid")
	return os.WriteFile(uidFile, []byte(fmt.Sprintf("%#x\n", maxUid)), 0600)
}

func maxOpenFilesWarning() {
	const (
		red    = "\x1b[31m"
		green  = "\x1b[32m"
		yellow = "\x1b[33m"
		reset  = "\x1b[0m"
	)
	maxOpenFiles, err := x.QueryMaxOpenFiles()
	if err != nil || maxOpenFiles < 1e6 {
		fmt.Println(green + "\nThe boot loader needs to open many files at once. This number depends" +
			" on the size of the data set loaded, the map file output size, and the level" +
			" of indexing. 100,000 is adequate for most data set sizes. See `man ulimit` for" +
			" details of how to change the limit.")
		if err != nil {
			fmt.Printf(red+"Nonfatal error: max open file limit could not be detected: %v\n"+reset, err)
		} else {
			fmt.Printf(yellow+"Current max open files limit: %d\n"+reset, maxOpenFiles)
		}
		fmt.Println()
	}
}

type options struct {
	DataFiles        string
	DataIPC          string
	GqlSchemaFile    string
	GqlType          string
	OutDir           string
	ReplaceOutDir    bool
	MapDir           string
	BufDir           string
	NumGoroutines    int
	MapBufSize       uint64
	PartitionBufSize int64
	SkipMapPhase     bool
	CleanupTmp       bool
	NumReducers      int
	Version          bool
	ZeroAddr         string
	HttpAddr         string
	IgnoreErrors     bool
	CustomTokenizers string
	ClientDir        string
	UniqueTypes      map[string]bool

	dqlSchema string
	gqlSchema *gqlSchema.Schema

	MapShards    int
	ReduceShards int

	Namespace uint64

	shardOutputDirs []string

	// ........... Badger options ..........
	// EncryptionKey is the key used for encryption. Enterprise only feature.
	EncryptionKey x.Sensitive
	// Badger options.
	Badger badger.Options
}

type state struct {
	opt           *options
	prog          *progress
	xids          *xidmap.XidMap
	dqlSchema     *schemaStore
	gqlSchema     *gqlSchema.Schema
	shards        *shardMap
	readerChunkCh chan *bytes.Buffer
	mapFileId     uint32 // Used atomically to name the output files of the mappers.
	dbs           []*badger.DB
	tmpDbs        []*badger.DB // Temporary DB to write the split lists to avoid ordering issues.
	writeTs       uint64       // All badger writes use this timestamp
	namespaces    *sync.Map    // To store the encountered namespaces.
}

func (st *state) objectModifier(m map[string]interface{}, typ *gqlSchema.Type) {
	if _, ok := m["uid"]; ok {
		// has uid, so use it.
		return
	}
	if st.opt.UniqueTypes[typ.Name()] {
		// is unique. We can just assign a UID here.
		m["uid"] = x.ToHexString(st.xids.AllocateUid())
		return
	}

	// Use the type of the object and the corresponding XID fields to
	// determine the UID.
	var comp []string
	for _, fd := range typ.XIDFields() {
		val, ok := m[fd.Name()]
		if ok {
			comp = append(comp, val.(string))
		}
	}
	m["uid"] = fmt.Sprintf("_:%s.%s", typ.Name(), strings.Join(comp, "|"))
}

type loader struct {
	*state
	mappers []*mapper
}

func newLoader(opt *options) *loader {
	if opt == nil {
		log.Fatalf("Cannot create loader with nil options.")
	}

	data, err := ioutil.ReadFile(opt.GqlSchemaFile)
	x.Check(err)
	x.Config.Lambda.Url = "http://dummy" // To satisfy some checks.
	handler, err := gqlSchema.NewHandler(string(data))
	x.Check(err)
	dgSchema := handler.DGSchema()
	fmt.Printf("schema parsed is:\n%s\n", dgSchema)
	opt.dqlSchema = handler.DGSchema()

	gsch, err := gqlSchema.FromString(handler.GQLSchema(), 0)
	x.Check(err)

	st := &state{
		opt:       opt,
		gqlSchema: gsch,
		prog:      newProgress(),
		shards:    newShardMap(opt.MapShards),
		// Lots of gz readers, so not much channel buffer needed.
		readerChunkCh: make(chan *bytes.Buffer, opt.NumGoroutines),
		writeTs:       getWriteTimestamp(),
		namespaces:    &sync.Map{},
	}
	st.dqlSchema = newSchemaStore(readSchema(opt), opt, st)
	ld := &loader{
		state:   st,
		mappers: make([]*mapper, opt.NumGoroutines),
	}
	for i := 0; i < opt.NumGoroutines; i++ {
		ld.mappers[i] = newMapper(st)
	}
	go ld.prog.report()
	return ld
}

func getWriteTimestamp() uint64 {
	// It seems better to use a fixed timestamp here. So, when Outserv loads up,
	// this timestamp would always be lower than whatever Outserv uses.
	// return x.Timestamp(uint64(time.Now().Unix())<<32, 0)
	return 1
}

func readSchema(opt *options) *schema.ParsedSchema {
	result, err := schema.ParseWithNamespace(opt.dqlSchema, opt.Namespace)
	x.Check(err)
	return result
}

func (ld *loader) blockingFileReader() {
	fs := filestore.NewFileStore(ld.opt.DataFiles)

	files := fs.FindDataFiles(ld.opt.DataFiles, []string{".json", ".json.gz"})
	if len(files) == 0 {
		fmt.Printf("No data files found in %s.\n", ld.opt.DataFiles)
		os.Exit(1)
	}

	// This is the main map loop.
	thr := y.NewThrottle(ld.opt.NumGoroutines)
	for i, file := range files {
		x.Check(thr.Do())
		fmt.Printf("Processing file (%d out of %d): %s\n", i+1, len(files), file)

		go func(file string) {
			defer thr.Done(nil)

			r, cleanup := fs.ChunkReader(file, nil)
			defer cleanup()

			chunk := chunker.NewChunker(ld.gqlSchema, 1000, ld.objectModifier)
			for {
				chunkBuf, err := chunk.Chunk(r)
				if chunkBuf != nil && chunkBuf.Len() > 0 {
					ld.readerChunkCh <- chunkBuf
				}
				if err == io.EOF {
					break
				} else if err != nil {
					x.Check(err)
				}
			}
		}(file)
	}
	x.Check(thr.Finish())
	close(ld.readerChunkCh)
}

func (ld *loader) blockingIPCReader() {
	fmt.Printf("Reading from IPC: %s\n", ld.opt.DataIPC)
	files := x.FindDataFiles(ld.opt.DataIPC, []string{".ipc"})
	fmt.Printf("Found files: %v\n", files)

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			// conn, err := net.Dial("unix", file)
			// x.Check(err)
			// defer conn.Close()
			fd, err := os.OpenFile(file, os.O_RDONLY, os.ModeNamedPipe)
			x.Check(err)
			defer fd.Close()

			r := bufio.NewReaderSize(fd, 32<<20)
			chunk := chunker.NewChunker(ld.gqlSchema, 1000, ld.objectModifier)
			for {
				chunkBuf, err := chunk.Chunk(r)
				if chunkBuf != nil && chunkBuf.Len() > 0 {
					ld.readerChunkCh <- chunkBuf
				}
				if err == io.EOF {
					fmt.Printf("io.EOF for IPC: %s\n", file)
					return
				} else if err != nil {
					x.Check(err)
				}
			}
		}(file)
	}
	wg.Wait()
	close(ld.readerChunkCh)
}

func (ld *loader) mapStage() {
	ld.prog.setPhase(mapPhase)
	var db *badger.DB
	if len(ld.opt.ClientDir) > 0 {
		x.Check(os.MkdirAll(ld.opt.ClientDir, 0700))

		var err error
		db, err = badger.Open(badger.DefaultOptions(ld.opt.ClientDir))
		x.Checkf(err, "Error while creating badger KV posting store")
	}
	ld.xids = xidmap.New(xidmap.XidMapOptions{
		DB:  db,
		Dir: filepath.Join(ld.opt.MapDir, bufferDir),
	})

	var mapperWg sync.WaitGroup
	mapperWg.Add(len(ld.mappers))
	for _, m := range ld.mappers {
		go func(m *mapper) {
			m.run()
			mapperWg.Done()
		}(m)
	}

	// Send the graphql triples
	if len(ld.opt.DataFiles) > 0 {
		ld.blockingFileReader()
	} else if len(ld.opt.DataIPC) > 0 {
		ld.blockingIPCReader()
	} else {
		fmt.Printf("No input provided. Must be one of files or socket")
		os.Exit(1)
	}

	// Wait for mappers to be done processing the input.
	mapperWg.Wait()

	// Allow memory to GC before the reduce phase.
	for i := range ld.mappers {
		ld.mappers[i] = nil
	}
	x.Check(ld.xids.Flush())
	if db != nil {
		x.Check(db.Close())
	}
	// ld.xids = nil
}

func parseGqlSchema(s string) map[uint64]*x.ExportedGQLSchema {
	schemaMap := make(map[uint64]*x.ExportedGQLSchema)

	var schemas []*x.ExportedGQLSchema
	if err := json.Unmarshal([]byte(s), &schemas); err != nil {
		fmt.Println("Error while decoding the graphql schema. Assuming it to be in format < 21.03.")
		schemaMap[x.GalaxyNamespace] = &x.ExportedGQLSchema{
			Namespace: x.GalaxyNamespace,
			Schema:    s,
		}
		return schemaMap
	}

	for _, schema := range schemas {
		if _, ok := schemaMap[schema.Namespace]; ok {
			fmt.Printf("Found multiple GraphQL schema for namespace %d.", schema.Namespace)
			continue
		}
		schemaMap[schema.Namespace] = schema
	}
	return schemaMap
}

func (ld *loader) reduceStage() {
	ld.prog.setPhase(reducePhase)

	r := reducer{
		state:     ld.state,
		streamIds: make(map[string]uint32),
	}
	x.Check(r.run())
}

func (ld *loader) writeSchema() {
	numDBs := uint32(len(ld.dbs))
	preds := make([][]string, numDBs)

	// Get all predicates that have data in some DB.
	m := make(map[string]struct{})
	for i, db := range ld.dbs {
		preds[i] = ld.dqlSchema.getPredicates(db)
		for _, p := range preds[i] {
			m[p] = struct{}{}
		}
	}

	// Find any predicates that don't have data in any DB
	// and distribute them among all the DBs.
	for p := range ld.dqlSchema.schemaMap {
		if _, ok := m[p]; !ok {
			i := adler32.Checksum([]byte(p)) % numDBs
			preds[i] = append(preds[i], p)
		}
	}

	// Write out each DB's final predicate list.
	for i, db := range ld.dbs {
		ld.dqlSchema.write(db, preds[i])
	}
}

func (ld *loader) cleanup() {
	for _, db := range ld.dbs {
		x.Check(db.Close())
	}
	for _, db := range ld.tmpDbs {
		opts := db.Opts()
		x.Check(db.Close())
		x.Check(os.RemoveAll(opts.Dir))
	}
	ld.prog.endSummary()
}
