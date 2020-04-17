package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	//"fmt"

	//"io"
	"log"
	//"github.com/davecgh/go-spew/spew"

	vb "github.com/molecula/vdsm/proto/vdsm"
	pb "github.com/pilosa/pilosa/v2/proto"
	"google.golang.org/grpc"
)

const maxMsgSize = 1024 * 1024 * 100 // 100 megs ought to be enough for anybody!

var flags struct {
	vdsm   bool // default is to use Pilosa
	phost  string
	vhost  string
	index  string
	pql    string
	sql    string
	unary  bool
	output bool
}

func init() {
	flag.BoolVar(&flags.vdsm, "vdsm", false, "true to send requests through the VDSM. false (default) uses Pilosa")
	flag.StringVar(&flags.phost, "phost", ":20101", "Pilosa host:port. used if -vdsm=false")
	flag.StringVar(&flags.vhost, "vhost", ":9000", "VDSM host:port. used if -vdsm=true")
	flag.StringVar(&flags.index, "index", "", "index (or vds) to use for pql queries")
	flag.StringVar(&flags.pql, "pql", "", "pql query string")
	flag.StringVar(&flags.sql, "sql", "", "sql query string")
	flag.BoolVar(&flags.unary, "unary", false, "true returns TableResponse. false (default) streams RowResponse")
	flag.BoolVar(&flags.output, "output", false, "true prints response. false (default) just performs the query for timing purposes")
	flag.Parse()
}

func main() {
	var hostport string = flags.phost
	if flags.vdsm {
		hostport = flags.vhost
	}

	conn, err := grpc.Dial(hostport,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return
	}
	defer conn.Close()

	if flags.vdsm {
		c := vb.NewMoleculaClient(conn)
		ctx := context.Background()

		// Unary: TableResponse
		if flags.unary {
			table, err := c.QueryPQLUnary(ctx, &vb.QueryPQLRequest{Vds: flags.index, Pql: flags.pql})
			if err != nil {
				log.Fatalf("failed to query: %s", err)
			}
			if flags.output {
				fmt.Println(table)
			}
			return
		}

		// Streaming: RowResponse
		stream, err := c.QueryPQL(ctx, &vb.QueryPQLRequest{Vds: flags.index, Pql: flags.pql})
		if err != nil {
			log.Fatalf("failed to query: %s", err)
		}

		i := 0
		for {
			row, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("failed to iterate: %s", err)
			}
			if flags.output {
				fmt.Println(row)
			}
			i++
		}
		if flags.output {
			fmt.Println("row count:", i)
		}
	} else {
		c := pb.NewPilosaClient(conn)
		ctx := context.Background()

		// Unary: TableResponse
		if flags.unary {
			table, err := c.QueryPQLUnary(ctx, &pb.QueryPQLRequest{Index: flags.index, Pql: flags.pql})
			if err != nil {
				log.Fatalf("failed to query: %s", err)
			}
			if flags.output {
				fmt.Println(table)
			}
			return
		}

		// Streaming: RowResponse
		stream, err := c.QueryPQL(ctx, &pb.QueryPQLRequest{Index: flags.index, Pql: flags.pql})
		if err != nil {
			log.Fatalf("failed to query: %s", err)
		}

		i := 0
		for {
			row, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("failed to iterate: %s", err)
			}
			if flags.output {
				fmt.Println(row)
			}
			i++
		}
		if flags.output {
			fmt.Println("row count:", i)
		}
	}
}
