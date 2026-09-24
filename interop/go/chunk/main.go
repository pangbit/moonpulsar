package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func checked(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "send" && os.Args[1] != "receive") {
		panic("usage: go run ./chunk send|receive")
	}
	base := os.Getenv("PULSAR_TOPIC")
	path := os.Getenv("PULSAR_PAYLOAD_FILE")
	if base == "" || path == "" {
		panic("PULSAR_TOPIC and PULSAR_PAYLOAD_FILE are required")
	}
	payload, err := os.ReadFile(path)
	checked(err)
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		url = "pulsar://127.0.0.1:6650"
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: url})
	checked(err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	codecs := []struct {
		name string
		kind pulsar.CompressionType
	}{
		{"lz4", pulsar.LZ4},
		{"zlib", pulsar.ZLib},
		{"zstd", pulsar.ZSTD},
		{"snappy", pulsar.SNAPPY},
	}
	for _, codec := range codecs {
		if os.Args[1] == "send" {
			producer, err := client.CreateProducer(pulsar.ProducerOptions{
				Topic:               base + "-go-" + codec.name,
				DisableBatching:     true,
				EnableChunking:      true,
				ChunkMaxMessageSize: 32768,
				CompressionType:     codec.kind,
			})
			checked(err)
			_, err = producer.Send(ctx, &pulsar.ProducerMessage{Payload: payload})
			checked(err)
			producer.Close()
		} else {
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:                       base + "-moonbit-" + codec.name,
				SubscriptionName:            fmt.Sprintf("moonpulsar-chunk-go-%s-%d", codec.name, time.Now().UnixNano()),
				SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			})
			checked(err)
			message, err := consumer.Receive(ctx)
			checked(err)
			if !bytes.Equal(message.Payload(), payload) {
				panic(fmt.Sprintf("%s: chunk payload mismatch (%d bytes)", codec.name, len(message.Payload())))
			}
			checked(consumer.Ack(message))
			consumer.Close()
		}
	}
	fmt.Printf("Go %s compressed chunks: LZ4, Zlib, Zstd, Snappy OK\n", os.Args[1])
}
