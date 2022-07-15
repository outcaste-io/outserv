# How to use this importer

## Notes from Field Test


Bulk loader could take 48 GBs of memory just to store the Hash -> Outserv UIDs
mapping. It is recommended to use a 64 GB RAM machine.

Assuming a fast connection to Geth, this import process is CPU bound. So,
recommendation is to use at least a 16-core machine, preferrably a 32-core.

```
$ curl localhost:8080/jemalloc
Num Allocated Bytes: 51 GiB [55150114759]
Allocators:

Allocations:
48 GiB at file: XidMap
3.3 GiB at file: buffer
```

Block 1 - 15M has around 1.6B transactions. This importer output 800 GBs of data
to the loader. Though, because it was using IPC, none of this data was stored on
disk.

```
BlockId: 14999304 Processed: 14999279 blocks 1609030063 txns [ 7h19m53s @ 17700 blocks/min ]
BlockId: 14999594 Processed: 14999574 blocks 1609079421 txns [ 7h19m54s @ 17700 blocks/min ]
BlockId: 14999896 Processed: 14999871 blocks 1609131079 txns [ 7h19m55s @ 17700 blocks/min ]
Process 6 wrote 99.78 GBs of data
Process 0 wrote 99.84 GBs of data
Process 1 wrote 99.78 GBs of data
Process 2 wrote 99.73 GBs of data
Process 4 wrote 99.83 GBs of data
Process 5 wrote 99.72 GBs of data
Process 3 wrote 99.77 GBs of data
Process 7 wrote 99.42 GBs of data
DONE
```

This resulted in 400 GBs (compressed) of map phase output on disk.

```
$ du -sh tmp
427G	tmp
```
