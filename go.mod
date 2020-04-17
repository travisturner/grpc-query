module grpc-query

go 1.13

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/molecula/vdsm v0.7.0
	github.com/pilosa/pilosa/v2 v2.0.0-alpha.1
	google.golang.org/grpc v1.28.1
)

replace github.com/pilosa/pilosa/v2 => ../../molecula/pilosa
