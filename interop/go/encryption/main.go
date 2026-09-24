package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/crypto"
)

func checked(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "send" && os.Args[1] != "receive" && os.Args[1] != "receive-empty") {
		panic("usage: go run ./encryption send|receive|receive-empty")
	}
	topic := os.Getenv("PULSAR_TOPIC")
	if topic == "" {
		panic("PULSAR_TOPIC is required")
	}
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		url = "pulsar://127.0.0.1:6650"
	}
	options := pulsar.ClientOptions{URL: url}
	if token := os.Getenv("PULSAR_TOKEN"); token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(token)
	}
	client, err := pulsar.NewClient(options)
	checked(err)
	defer client.Close()
	keys := crypto.NewFileKeyReader("../../testdata/encryption/rsa_public.pem", "../../testdata/encryption/rsa_private.pem")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if os.Args[1] == "send" {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic + "-go", DisableBatching: true,
			Encryption: &pulsar.ProducerEncryptionInfo{
				KeyReader: keys, Keys: []string{"test-key"},
			},
		})
		checked(err)
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte("encrypted-from-go")})
		checked(err)
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte{}})
		checked(err)
		producer.Close()
		fmt.Println("Go encrypted send and empty payload OK")
		return
	}
	if os.Args[1] == "receive-empty" {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       topic + "-empty",
			SubscriptionName:            fmt.Sprintf("moonpulsar-crypto-empty-go-%d", time.Now().UnixNano()),
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			Decryption:                  &pulsar.MessageDecryptionInfo{KeyReader: keys},
		})
		checked(err)
		message, err := consumer.Receive(ctx)
		checked(err)
		if len(message.Payload()) != 16 {
			panic(fmt.Sprintf("expected pinned Go client to expose 16-byte encrypted empty payload, got %d bytes", len(message.Payload())))
		}
		checked(consumer.Ack(message))
		consumer.Close()
		fmt.Println("Pinned Go client empty-payload decryption limitation reproduced")
		return
	}
	for suffix, expected := range map[string][]string{
		"-moonbit":    {"encrypted-from-moonbit"},
		"-compressed": {"encrypted-zlib"},
		"-batch":      {"encrypted-batch-first", "encrypted-batch-second"},
	} {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       topic + suffix,
			SubscriptionName:            fmt.Sprintf("moonpulsar-crypto-go-%d", time.Now().UnixNano()),
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			Decryption:                  &pulsar.MessageDecryptionInfo{KeyReader: keys},
		})
		checked(err)
		for _, want := range expected {
			message, err := consumer.Receive(ctx)
			checked(err)
			if string(message.Payload()) != want {
				panic(fmt.Sprintf("unexpected decrypted payload: %q", message.Payload()))
			}
			checked(consumer.Ack(message))
		}
		consumer.Close()
	}
	fmt.Println("MoonBit-to-Go encrypted single, Zlib and batch decode OK")
}
