package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {

	batchSize := flag.Int("batch", 1000, "batch size")
	uri := flag.String("uri", "", "uri")
	database := flag.String("db", "", "db")
	collection := flag.String("collection", "", "collection to watch")
	watchStart := flag.String("resumetoken", "", "token to start monitoring from")

	timeout := flag.Duration("timeout", 0, "how long to monitor")
	flag.Parse()

	// Create a new client and connect to the server
	log.Printf("connecting to %s", *uri)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*uri))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Ping the primary
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	log.Printf("Successfully connected and pinged.")
	log.Printf("Watching %s.%s starting from token %s batch_size=%d", *database, *collection, *watchStart, *batchSize)

	opts := &options.ChangeStreamOptions{
		BatchSize: toInt32Ptr(batchSize),
	}
	if *watchStart != "" {
		opts.ResumeAfter = map[string]string{
			"_data": *watchStart,
		}

	}
	stream, err := client.Database(*database).Collection(*collection).
		Watch(context.TODO(),
			mongo.Pipeline{},
			opts)
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()
	if *timeout > 0 {
		mctx, cancelFn := context.WithTimeout(ctx, *timeout)
		defer cancelFn()
		ctx = mctx
	}

	for stream.Next(ctx) {
		fmt.Printf(`{"received": "%s", "size": %d, "data": %s}
`,
			time.Now().UTC().Format(time.RFC3339),
			len(stream.Current),
			stream.Current,
		)
	}

}

func toInt32Ptr(v *int) *int32 {
	if v == nil {
		return nil
	}

	p := int32(*v)
	return &p
}
