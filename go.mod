module github.com/outcaste-io/outserv

go 1.16

// replace github.com/outcaste-io/gqlparser/v2 => /home/mrjn/source/gqlparser

require (
	cloud.google.com/go/storage v1.15.0
	contrib.go.opencensus.io/exporter/jaeger v0.1.0
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/DataDog/datadog-go v0.0.0-20190425163447-40bafcb5f6c1 // indirect
	github.com/DataDog/opencensus-go-exporter-datadog v0.0.0-20190503082300-0f32ad59ab08
	github.com/Masterminds/semver/v3 v3.1.0
	github.com/Microsoft/go-winio v0.4.15 // indirect
	github.com/Shopify/sarama v1.27.2
	github.com/blevesearch/bleve v1.0.13
	github.com/btcsuite/btcd v0.22.0-beta // indirect
	github.com/cespare/cp v1.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/dgraph-io/graphql-transport-ws v0.0.0-20210511143556-2cef522f1f15
	github.com/dgraph-io/simdjson-go v0.3.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgrijalva/jwt-go/v4 v4.0.0-preview1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v1.13.1
	github.com/dustin/go-humanize v1.0.0
	github.com/ethereum/go-ethereum v1.10.16
	github.com/gballet/go-libpcsclite v0.0.0-20191108122812-4678299bea08 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/geo v0.0.0-20190916061304-5b978397cfec
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/codesearch v1.0.0
	github.com/google/flatbuffers v1.11.0
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/gorilla/websocket v1.4.2
	github.com/graph-gophers/graphql-go v1.3.0
	github.com/klauspost/compress v1.11.7
	github.com/klauspost/cpuid/v2 v2.0.11 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/minio/minio-go/v6 v6.0.55
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/outcaste-io/dgo/v210 v210.0.0-20220225180226-43bd1b427e86
	github.com/outcaste-io/gqlgen v0.13.3
	github.com/outcaste-io/gqlparser/v2 v2.2.4-0.20220815195457-c596227b68e0
	github.com/outcaste-io/ristretto v0.1.1-0.20220404170646-118eb5c81eac
	github.com/outcaste-io/sroar v0.0.0-20220207092908-fa5c189ea338
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.2.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/tsdb v0.10.0 // indirect
	github.com/rjeczalik/notify v0.9.2 // indirect
	github.com/sergi/go-diff v1.2.0
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/shirou/gopsutil/v3 v3.22.2
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.7.1
	github.com/status-im/keycard-go v0.0.0-20200402102358-957c09536969 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/tinylib/msgp v1.1.5 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/twpayne/go-geom v1.0.5
	github.com/tyler-smith/go-bip39 v1.1.0 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.0.0-20190228193606-a943ad0ee4c9
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20220213190939-1e6e3497d506
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
	golang.org/x/text v0.3.7
	golang.org/x/tools v0.1.6-0.20210802203754-9b21a8868e16
	google.golang.org/api v0.46.0
	google.golang.org/genproto v0.0.0-20210510173355-fb37daa5cd7a // indirect
	google.golang.org/grpc v1.37.1
	google.golang.org/grpc/examples v0.0.0-20210518002758-2713b77e8526 // indirect
	gopkg.in/DataDog/dd-trace-go.v1 v1.13.1 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	honnef.co/go/tools v0.2.0 // indirect
	src.techknowlogick.com/xgo v1.4.1-0.20210311222705-d25c33fcd864
)
