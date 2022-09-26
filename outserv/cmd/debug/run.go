// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package debug

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/DataDog/zstd"
	"github.com/dustin/go-humanize"
	"github.com/outcaste-io/outserv/badger"
	bpb "github.com/outcaste-io/outserv/badger/pb"
	"github.com/outcaste-io/ristretto/z"

	"github.com/outcaste-io/outserv/codec"
	"github.com/outcaste-io/outserv/ee"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/raftwal"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
)

var (
	// Debug is the sub-command invoked when calling "outserv debug"
	Debug x.SubCommand
	opt   flagOptions
)

type flagOptions struct {
	testZstd      bool
	testOpenFiles bool
	keyLookup     string
	rollupKey     string
	parseKey      string
	keyHistory    bool
	predicate     string
	prefix        string
	readOnly      bool
	pdir          string
	itemMeta      bool
	readTs        uint64
	sizeHistogram bool
	namespace     uint64
	key           x.Sensitive
	onlySummary   bool

	// Options related to the WAL.
	wdir           string
	wtruncateUntil uint64
	wsetSnapshot   string
}

func init() {
	Debug.Cmd = &cobra.Command{
		Use:   "debug",
		Short: "Debug Outserv instance",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
		Annotations: map[string]string{"group": "debug"},
	}
	Debug.Cmd.SetHelpTemplate(x.NonRootTemplate)

	flag := Debug.Cmd.Flags()
	flag.BoolVar(&opt.testZstd, "zstd", false, "Reads from stdin, decompress and write to stdout")
	flag.BoolVar(&opt.testOpenFiles, "ulimit", false, "Test how many open files can we have.")
	flag.BoolVar(&opt.itemMeta, "item", true, "Output item meta as well. Set to false for diffs.")
	flag.Uint64Var(&opt.readTs, "at", math.MaxUint64, "Set read timestamp for all txns.")
	flag.BoolVarP(&opt.readOnly, "readonly", "o", false, "Open in read only mode.")
	flag.StringVarP(&opt.predicate, "pred", "r", "", "Only output specified predicate.")
	flag.Uint64VarP(&opt.namespace, "ns", "", 0, "Which namespace to use.")
	flag.StringVarP(&opt.prefix, "prefix", "", "", "Uses a hex prefix.")
	flag.StringVarP(&opt.keyLookup, "lookup", "l", "", "Hex of key to lookup.")
	flag.StringVar(&opt.rollupKey, "rollup", "", "Hex of key to rollup.")
	flag.BoolVarP(&opt.keyHistory, "history", "y", false, "Show all versions of a key.")
	flag.StringVarP(&opt.pdir, "postings", "p", "", "Directory where posting lists are stored.")
	flag.BoolVar(&opt.sizeHistogram, "histogram", false,
		"Show a histogram of the key and value sizes.")
	flag.BoolVar(&opt.onlySummary, "only-summary", false,
		"If true, only show the summary of the p directory.")
	flag.StringVarP(&opt.parseKey, "parse", "", "", "Parse and output the key")

	// Flags related to WAL.
	flag.StringVarP(&opt.wdir, "wal", "w", "", "Directory where Raft write-ahead logs are stored.")
	flag.Uint64VarP(&opt.wtruncateUntil, "truncate", "t", 0,
		"Remove data from Raft entries until but not including this index.")
	flag.StringVarP(&opt.wsetSnapshot, "snap", "s", "",
		"Set snapshot term,index,readts to this. Value must be comma-separated list containing"+
			" the value for these vars in that order.")
	ee.RegisterEncFlag(flag)
}

func toInt(o *pb.Posting) int {
	out, err := types.Convert(types.Sval(o.Value), types.TypeString)
	x.Check(err)
	val := out.Value.(string)
	a, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return a
}

func uidToVal(itr *badger.Iterator, prefix string) map[uint64]int {
	keys := make(map[uint64]int)
	var lastKey []byte
	for itr.Rewind(); itr.Valid(); {
		item := itr.Item()
		if bytes.Equal(lastKey, item.Key()) {
			itr.Next()
			continue
		}
		lastKey = append(lastKey[:0], item.Key()...)
		pk, err := x.Parse(item.Key())
		x.Check(err)
		if !pk.IsData() || !strings.HasPrefix(x.ParseAttr(pk.Attr), prefix) {
			continue
		}
		if pk.IsSchema() {
			continue
		}
		if pk.StartUid > 0 {
			// This key is part of a multi-part posting list. Skip it and only read
			// the main key, which is the entry point to read the whole list.
			continue
		}

		pl, err := posting.ReadPostingList(item.KeyCopy(nil), itr)
		if err != nil {
			log.Fatalf("Unable to read posting list: %v", err)
		}
		err = pl.Iterate(math.MaxUint64, 0, func(o *pb.Posting) error {
			out, err := types.Convert(types.Sval(o.Value), types.TypeString)
			x.Check(err)
			key := out.Value.(string)
			k, err := strconv.Atoi(key)
			x.Check(err)
			keys[pk.Uid] = k
			// fmt.Printf("Type: %v Uid=%d key=%s. commit=%d hex %x\n",
			// 	o.ValType, pk.Uid, key, o.CommitTs, lastKey)
			return nil
		})
		x.Checkf(err, "during iterate")
	}
	return keys
}

func history(lookup []byte, itr *badger.Iterator) {
	var buf bytes.Buffer
	pk, err := x.Parse(lookup)
	x.Check(err)
	fmt.Fprintf(&buf, "==> key: %x. PK: %+v\n", lookup, pk)
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		if !bytes.Equal(item.Key(), lookup) {
			break
		}

		fmt.Fprintf(&buf, "ts: %d", item.Version())
		x.Check2(buf.WriteString(" {item}"))
		if item.IsDeletedOrExpired() {
			x.Check2(buf.WriteString("{deleted}"))
		}
		if item.DiscardEarlierVersions() {
			x.Check2(buf.WriteString("{discard}"))
		}
		val, err := item.ValueCopy(nil)
		x.Check(err)

		meta := item.UserMeta()
		if meta&posting.BitCompletePosting > 0 {
			x.Check2(buf.WriteString("{complete}"))
		}
		if meta&posting.BitDeltaPosting > 0 {
			x.Check2(buf.WriteString("{delta}"))
		}
		if meta&posting.BitEmptyPosting > 0 {
			x.Check2(buf.WriteString("{empty}"))
		}
		fmt.Fprintln(&buf)
		if meta&posting.BitDeltaPosting > 0 {
			plist := &pb.PostingList{}
			x.Check(plist.Unmarshal(val))
			for _, p := range plist.Postings {
				appendPosting(&buf, p)
			}
		}
		if meta&posting.BitCompletePosting > 0 {
			var plist pb.PostingList
			x.Check(plist.Unmarshal(val))

			for _, p := range plist.Postings {
				appendPosting(&buf, p)
			}

			r := codec.FromBytes(plist.Bitmap)
			fmt.Fprintf(&buf, " Num uids = %d. Size = %d\n",
				r.GetCardinality(), len(plist.Bitmap))

			itr := r.ManyIterator()
			uids := make([]uint64, 256)
			for {
				num := itr.NextMany(uids)
				if num == 0 {
					break
				}
				for _, uid := range uids[:num] {
					fmt.Fprintf(&buf, " Uid = %#x\n", uid)
				}
			}
		}
		x.Check2(buf.WriteString("\n"))
	}
	fmt.Println(buf.String())
}

func appendPosting(w io.Writer, o *pb.Posting) {
	fmt.Fprintf(w, " Uid: %#x Op: %d ", o.Uid, o.Op)

	if len(o.Value) > 0 {
		fmt.Fprintf(w, " Type: %s. ", types.TypeID(o.Value[0]))
		vals, err := types.FromList(o.Value)
		if err != nil {
			fmt.Fprintf(w, " Error: %v", err)
		} else {
			for _, val := range vals {
				out, err := types.Convert(val, types.TypeString)
				if err != nil {
					fmt.Fprintf(w, " Value: %q Error: %v", o.Value, err)
				} else {
					fmt.Fprintf(w, " String Value: %q", out.Value)
				}
			}
		}
	}
	fmt.Fprintln(w, "")
}
func rollupKey(db *badger.DB) {
	txn := db.NewReadTxn(opt.readTs)
	defer txn.Discard()

	key, err := hex.DecodeString(opt.rollupKey)
	x.Check(err)

	iopts := badger.DefaultIteratorOptions
	iopts.AllVersions = true
	iopts.PrefetchValues = false
	itr := txn.NewKeyIterator(key, iopts)
	defer itr.Close()

	itr.Rewind()
	if !itr.Valid() {
		log.Fatalf("Unable to seek to key: %s", hex.Dump(key))
	}

	item := itr.Item()
	// Don't need to do anything if the bitdelta is not set.
	if item.UserMeta()&posting.BitDeltaPosting == 0 {
		fmt.Printf("First item has UserMeta:[b%04b]. Nothing to do\n", item.UserMeta())
		return
	}
	pl, err := posting.ReadPostingList(item.KeyCopy(nil), itr)
	x.Check(err)

	alloc := z.NewAllocator(32<<20, "Debug.RollupKey")
	defer alloc.Release()

	kvs, err := pl.Rollup(alloc)
	x.Check(err)

	wb := db.NewWriteBatch()
	x.Check(wb.WriteList(&bpb.KVList{Kv: kvs}))
	x.Check(wb.Flush())
}

func lookup(db *badger.DB) {
	txn := db.NewReadTxn(opt.readTs)
	defer txn.Discard()

	iopts := badger.DefaultIteratorOptions
	iopts.AllVersions = true
	iopts.PrefetchValues = false
	itr := txn.NewIterator(iopts)
	defer itr.Close()

	key, err := hex.DecodeString(opt.keyLookup)
	if err != nil {
		log.Fatal(err)
	}
	itr.Seek(key)
	if !itr.Valid() {
		log.Fatalf("Unable to seek to key: %s", hex.Dump(key))
	}

	if opt.keyHistory {
		history(key, itr)
		return
	}

	item := itr.Item()
	pl, err := posting.ReadPostingList(item.KeyCopy(nil), itr)
	if err != nil {
		log.Fatal(err)
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, " Key: %x", item.Key())
	fmt.Fprintf(&buf, " Length: %d", pl.Length(math.MaxUint64, 0))

	splits := pl.PartSplits()
	isMultiPart := len(splits) > 0
	fmt.Fprintf(&buf, " Is multi-part list? %v", isMultiPart)
	if isMultiPart {
		fmt.Fprintf(&buf, " Start UID of parts: %v\n", splits)
	}

	err = pl.Iterate(math.MaxUint64, 0, func(o *pb.Posting) error {
		appendPosting(&buf, o)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(buf.String())
}

// Current format is like:
// {i} attr: name term: [8] woods  ts: 535 item: [28, b0100] sz: 81 dcnt: 3 key: 00000...6f6f6473
// Fix the TestBulkLoadMultiShard accordingly, if the format changes.
func printKeys(db *badger.DB) {
	var prefix []byte
	if len(opt.predicate) > 0 {
		pred := x.NamespaceAttr(opt.namespace, opt.predicate)
		prefix = x.PredicatePrefix(pred)
	} else if len(opt.prefix) > 0 {
		p, err := hex.DecodeString(opt.prefix)
		x.Check(err)
		prefix = p
	}
	fmt.Printf("prefix = %s\n", hex.Dump(prefix))
	stream := db.NewStreamAt(opt.readTs)
	stream.Prefix = prefix
	var total uint64
	stream.KeyToList = func(key []byte, itr *badger.Iterator) (*bpb.KVList, error) {
		item := itr.Item()
		pk, err := x.Parse(key)
		x.Check(err)
		var buf bytes.Buffer
		// Don't use a switch case here. Because multiple of these can be true. In particular,
		// IsSchema can be true alongside IsData.
		if pk.IsData() {
			x.Check2(buf.WriteString("{d}"))
		}
		if pk.IsIndex() {
			x.Check2(buf.WriteString("{i}"))
		}
		if pk.IsCount() {
			x.Check2(buf.WriteString("{c}"))
		}
		if pk.IsSchema() {
			x.Check2(buf.WriteString("{s}"))
		}
		ns, attr := x.ParseNamespaceAttr(pk.Attr)
		x.Check2(buf.WriteString(fmt.Sprintf(" ns: %#x ", ns)))
		x.Check2(buf.WriteString(" attr: " + attr))
		if len(pk.Term) > 0 {
			fmt.Fprintf(&buf, " term: [%d] %s ", pk.Term[0], pk.Term[1:])
		}
		if pk.Count > 0 {
			fmt.Fprintf(&buf, " count: %d ", pk.Count)
		}
		if pk.Uid > 0 {
			fmt.Fprintf(&buf, " uid: %#x ", pk.Uid)
		}
		if pk.StartUid > 0 {
			fmt.Fprintf(&buf, " startUid: %#x ", pk.StartUid)
		}

		if opt.itemMeta {
			fmt.Fprintf(&buf, " ts: %d", item.Version())
			fmt.Fprintf(&buf, " item: [%d, b%04b]", item.EstimatedSize(), item.UserMeta())
		}

		var sz, deltaCount int64
	LOOP:
		for ; itr.ValidForPrefix(prefix); itr.Next() {
			item := itr.Item()
			if !bytes.Equal(item.Key(), key) {
				break
			}
			if item.IsDeletedOrExpired() {
				x.Check2(buf.WriteString(" {v.del}"))
				break
			}
			switch item.UserMeta() {
			// This is rather a default case as one of the 4 bit must be set.
			case posting.BitCompletePosting, posting.BitEmptyPosting, posting.BitSchemaPosting,
				posting.BitForbidPosting:
				sz += item.EstimatedSize()
				break LOOP
			case posting.BitDeltaPosting:
				sz += item.EstimatedSize()
				deltaCount++
			default:
				fmt.Printf("No user meta found for key: %s\n", hex.EncodeToString(key))
			}
			if item.DiscardEarlierVersions() {
				x.Check2(buf.WriteString(" {v.las}"))
				break
			}
		}
		var invalidSz, invalidCount uint64
		// skip all the versions of key
		for ; itr.ValidForPrefix(prefix); itr.Next() {
			item := itr.Item()
			if !bytes.Equal(item.Key(), key) {
				break
			}
			invalidSz += uint64(item.EstimatedSize())
			invalidCount++
		}

		fmt.Fprintf(&buf, " sz: %d dcnt: %d", sz, deltaCount)
		if invalidCount > 0 {
			fmt.Fprintf(&buf, " isz: %d icount: %d", invalidSz, invalidCount)
		}
		fmt.Fprintf(&buf, " key: %s", hex.EncodeToString(key))
		// If total size is more than 1 GB or we have more than 1 million keys, flag this key.
		if uint64(sz)+invalidSz > (1<<30) || uint64(deltaCount)+invalidCount > 10e6 {
			fmt.Fprintf(&buf, " [HEAVY]")
		}
		buf.WriteRune('\n')
		list := &bpb.KVList{}
		list.Kv = append(list.Kv, &bpb.KV{
			Value: buf.Bytes(),
		})
		// Don't call fmt.Println here. It is much slower.
		return list, nil
	}

	w := bufio.NewWriterSize(os.Stdout, 16<<20)
	stream.Send = func(buf *z.Buffer) error {
		var count int
		err := buf.SliceIterate(func(s []byte) error {
			var kv bpb.KV
			if err := kv.Unmarshal(s); err != nil {
				return err
			}
			x.Check2(w.Write(kv.Value))
			count++
			return nil
		})
		atomic.AddUint64(&total, uint64(count))
		return err
	}
	x.Check(stream.Orchestrate(context.Background()))
	w.Flush()
	fmt.Println()
	fmt.Printf("Found %d keys\n", atomic.LoadUint64(&total))
}

func sizeHistogram(db *badger.DB) {
	txn := db.NewReadTxn(opt.readTs)
	defer txn.Discard()

	iopts := badger.DefaultIteratorOptions
	iopts.PrefetchValues = false
	itr := txn.NewIterator(iopts)
	defer itr.Close()

	// Generate distribution bounds. Key sizes are not greater than 2^16 while
	// value sizes are not greater than 1GB (2^30).
	keyBounds := z.HistogramBounds(5, 16)
	valueBounds := z.HistogramBounds(5, 30)

	// Initialize exporter.
	keySizeHistogram := z.NewHistogramData(keyBounds)
	valueSizeHistogram := z.NewHistogramData(valueBounds)

	// Collect key and value sizes.
	var prefix []byte
	if len(opt.predicate) > 0 {
		prefix = x.PredicatePrefix(opt.predicate)
	}
	var loop int
	for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
		item := itr.Item()

		keySizeHistogram.Update(int64(len(item.Key())))
		valueSizeHistogram.Update(item.ValueSize())

		loop++
	}

	fmt.Printf("prefix = %s\n", hex.Dump(prefix))
	fmt.Printf("Found %d keys\n", loop)
	fmt.Printf("\nHistogram of key sizes (in bytes) %s\n", keySizeHistogram.String())
	fmt.Printf("\nHistogram of value sizes (in bytes) %s\n", valueSizeHistogram.String())
}

func printAlphaProposal(buf *bytes.Buffer, pr *pb.Proposal) {
	if pr == nil {
		return
	}

	switch {
	case pr.Mutations != nil:
		fmt.Fprintf(buf, " Mutation . Edges: %d .", len(pr.Mutations.Edges))
		if len(pr.Mutations.Edges) > 0 {
		} else {
			fmt.Fprintf(buf, " Mutation: %+v .", pr.Mutations)
		}
	case len(pr.Kv) > 0:
		fmt.Fprintf(buf, " KV . Size: %d ", len(pr.Kv))
	case pr.Snapshot != nil:
		fmt.Fprintf(buf, " Snapshot . %+v ", pr.Snapshot)
	}
}

func printZeroProposal(buf *bytes.Buffer, zpr *pb.ZeroProposal) {
	if zpr == nil {
		return
	}

	switch {
	case len(zpr.SnapshotTs) > 0:
		fmt.Fprintf(buf, " Snapshot: %+v .", zpr.SnapshotTs)
	case zpr.Member != nil:
		fmt.Fprintf(buf, " Member: %+v .", zpr.Member)
	case zpr.Tablets != nil:
		fmt.Fprintf(buf, " Tablets: %+v .", zpr.Tablets)
	case zpr.NumUids > 0:
		fmt.Fprintf(buf, " NumUids: %d .", zpr.NumUids)
	case zpr.NumNsids > 0:
		fmt.Fprintf(buf, " NumNsids: %d .", zpr.NumNsids)
	default:
		fmt.Fprintf(buf, " Proposal: %+v .", zpr)
	}
}

func printSummary(db *badger.DB) {
	nsFromKey := func(key []byte) uint64 {
		pk, err := x.Parse(key)
		if err != nil {
			// Some of the keys are badger's internal and couldn't be parsed.
			// Hence, the error is expected in that case.
			fmt.Printf("Unable to parse key: %#x\n", key)
			return x.GalaxyNamespace
		}
		return x.ParseNamespace(pk.Attr)
	}
	banned := db.BannedNamespaces()
	bannedNs := make(map[uint64]struct{})
	for _, ns := range banned {
		bannedNs[ns] = struct{}{}
	}

	tables := db.Tables()
	levelSizes := make([]uint64, len(db.Levels()))
	nsSize := make(map[uint64]uint64)
	for _, tab := range tables {
		levelSizes[tab.Level] += uint64(tab.OnDiskSize)
		if nsFromKey(tab.Left) == nsFromKey(tab.Right) {
			nsSize[nsFromKey(tab.Left)] += uint64(tab.OnDiskSize)
		}
	}

	fmt.Println("[SUMMARY]")
	totalSize := uint64(0)
	for i, sz := range levelSizes {
		fmt.Printf("Level %d size: %12s\n", i, humanize.IBytes(sz))
		totalSize += sz
	}
	fmt.Printf("Total SST size: %12s\n", humanize.IBytes(totalSize))
	fmt.Println()
	for ns, sz := range nsSize {
		fmt.Printf("Namespace %#x size: %12s", ns, humanize.IBytes(sz))
		if _, ok := bannedNs[ns]; ok {
			fmt.Printf(" (banned)")
		}
		fmt.Println()
	}
	fmt.Println()
}

func testOpenFilesLimit() {
	tmp, err := ioutil.TempDir("", "outserv")
	x.Check(err)

	N := 100000
	var open []*os.File
	for i := 0; i < N; i++ {
		fd, err := os.Create(path.Join(tmp, fmt.Sprintf("%05d", i)))
		if err != nil {
			fmt.Printf("Got error when opening file: %v\n", err)
			break
		}
		open = append(open, fd)
		if len(open)%100 == 0 {
			fmt.Printf("Opened %05d files\n", len(open))
		}
	}
	for _, fd := range open {
		fd.Close()
	}
	os.RemoveAll(tmp)
	fmt.Printf("Num files a process can open: %d\n", len(open))
	if len(open) == N {
		fmt.Println("Open File Limit Test: PASS")
	} else {
		fmt.Println("Open File Limit Test: FAIL")
	}
}

func testZstdDecompress() {
	reader := zstd.NewReader(os.Stdin)
	defer reader.Close()

	br := bufio.NewReaderSize(reader, 1<<20)
	buf := make([]byte, 64<<20)

	for {
		n, err := br.Read(buf[:cap(buf)])
		if err == io.EOF {
			return
		}
		x.Check(err)
		_, err = os.Stdout.Write(buf[:n])
		x.Check(err)
	}
}

func run() {
	if opt.testZstd {
		testZstdDecompress()
		return
	}
	if opt.testOpenFiles {
		testOpenFilesLimit()
		return
	}
	if len(opt.parseKey) > 0 {
		key, err := hex.DecodeString(opt.parseKey)
		if err != nil {
			log.Fatalf("Got error when decoding key: %v\n", err)
		}
		pk, err := x.Parse(key)
		if err != nil {
			log.Fatalf("Got error when parsing key: %v\n", err)
		}
		fmt.Printf("Parsed key: %+v\n", pk)
		return
	}

	go func() {
		for i := 8080; i < 9080; i++ {
			fmt.Printf("Listening for /debug HTTP requests at port: %d\n", i)
			if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", i), nil); err != nil {
				fmt.Println("Port busy. Trying another one...")
				continue
			}
		}
	}()

	dir := opt.pdir
	isWal := false
	if len(dir) == 0 {
		dir = opt.wdir
		isWal = true
	}
	keys, err := ee.GetKeys(Debug.Conf)
	x.Check(err)
	opt.key = keys.EncKey

	if isWal {
		store, err := raftwal.InitEncrypted(dir, opt.key)
		x.Check(err)
		if err := handleWal(store); err != nil {
			fmt.Printf("\nGot error while handling WAL: %v\n", err)
		}
		return
	}

	bopts := badger.DefaultOptions(dir).
		WithReadOnly(opt.readOnly).
		WithEncryptionKey(opt.key).
		WithBlockCacheSize(1 << 30).
		WithIndexCacheSize(1 << 30).
		WithExternalMagic(x.MagicVersion).
		WithNamespaceOffset(x.NamespaceOffset) // We don't want to see the banned data.

	x.AssertTruef(len(bopts.Dir) > 0, "No posting or wal dir specified.")
	fmt.Printf("Opening DB: %s\n", bopts.Dir)

	db, err := badger.Open(bopts)
	x.Check(err)
	// Not using posting list cache
	posting.Init(db, 0)
	defer db.Close()

	printSummary(db)
	if opt.onlySummary {
		return
	}

	// Commenting the following out because on large Badger DBs, this can take a LONG time.
	// min, max := getMinMax(db, opt.readTs)
	// fmt.Printf("Min commit: %d. Max commit: %d, w.r.t %d\n", min, max, opt.readTs)

	switch {
	case len(opt.rollupKey) > 0:
		rollupKey(db)
	case len(opt.keyLookup) > 0:
		lookup(db)
	case opt.sizeHistogram:
		sizeHistogram(db)
	default:
		printKeys(db)
	}
}
