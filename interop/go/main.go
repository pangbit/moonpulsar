package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const descriptorBase64 = "CmwKKHRlc3RkYXRhL3Byb3RvYnVmX25hdGl2ZV9rZXlfdmFsdWUucHJvdG8SDHB1bHNhci5wcm90byIyCghLZXlWYWx1ZRIQCgNrZXkYASACKAlSA2tleRIUCgV2YWx1ZRgCIAIoCVIFdmFsdWU="

func checked(err error) {
	if err != nil {
		panic(err)
	}
}

func message(descriptor protoreflect.MessageDescriptor, source string) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(descriptor)
	msg.Set(descriptor.Fields().ByName("key"), protoreflect.ValueOfString("source"))
	msg.Set(descriptor.Fields().ByName("value"), protoreflect.ValueOfString(source))
	return msg
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "send" && os.Args[1] != "receive") {
		panic("usage: go run . send|receive")
	}
	topic := os.Getenv("PULSAR_TOPIC")
	if topic == "" {
		panic("PULSAR_TOPIC is required")
	}
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		url = "pulsar://127.0.0.1:6650"
	}
	bytes, err := base64.StdEncoding.DecodeString(descriptorBase64)
	checked(err)
	set := &descriptorpb.FileDescriptorSet{}
	checked(proto.Unmarshal(bytes, set))
	file, err := protodesc.NewFile(set.File[0], nil)
	checked(err)
	descriptor := file.Messages().ByName("KeyValue")
	schema := pulsar.NewProtoNativeSchemaWithMessage(message(descriptor, ""), nil)
	options := pulsar.ClientOptions{URL: url}
	if token := os.Getenv("PULSAR_TOKEN"); token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(token)
	}
	client, err := pulsar.NewClient(options)
	checked(err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if os.Args[1] == "send" {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic, Schema: schema})
		checked(err)
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{Value: message(descriptor, "go")})
		checked(err)
		producer.Close()
		int64Producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic + "-int64", Schema: pulsar.NewInt64Schema(nil),
			BatchingMaxMessages: 2, BatchingMaxPublishDelay: time.Second,
		})
		checked(err)
		results := make(chan error, 2)
		for _, value := range []int64{-123456789, 42} {
			int64Producer.SendAsync(ctx, &pulsar.ProducerMessage{Value: value},
				func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, sendErr error) { results <- sendErr })
		}
		checked(int64Producer.FlushWithCtx(ctx))
		for range 2 {
			checked(<-results)
		}
		int64Producer.Close()
		fmt.Println("Go Protobuf Native send OK")
		fmt.Println("Go INT64 send OK")
		return
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            fmt.Sprintf("moonpulsar-go-interop-%d", time.Now().UnixNano()),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Schema:                      schema,
	})
	checked(err)
	defer consumer.Close()
	for {
		incoming, err := consumer.Receive(ctx)
		checked(err)
		decoded := dynamicpb.NewMessage(descriptor)
		checked(incoming.GetSchemaValue(decoded))
		if decoded.Get(descriptor.Fields().ByName("value")).String() == "moonbit" {
			if decoded.Get(descriptor.Fields().ByName("key")).String() != "source" {
				panic("unexpected MoonBit payload")
			}
			checked(consumer.Ack(incoming))
			fmt.Println("MoonBit-to-Go Protobuf Native decode OK")
			break
		}
		checked(consumer.Ack(incoming))
	}
	int64Consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic + "-int64",
		SubscriptionName:            fmt.Sprintf("moonpulsar-go-int64-interop-%d", time.Now().UnixNano()),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Schema:                      pulsar.NewInt64Schema(nil),
	})
	checked(err)
	defer int64Consumer.Close()
	for {
		incoming, err := int64Consumer.Receive(ctx)
		checked(err)
		var value int64
		checked(incoming.GetSchemaValue(&value))
		checked(int64Consumer.Ack(incoming))
		if value == 987654321 {
			fmt.Println("MoonBit-to-Go INT64 decode OK")
			break
		}
	}
	batchConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic + "-batch",
		SubscriptionName:            fmt.Sprintf("moonpulsar-go-batch-interop-%d", time.Now().UnixNano()),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Schema:                      pulsar.NewStringSchema(nil),
	})
	checked(err)
	defer batchConsumer.Close()
	for _, expected := range []string{"first", "second"} {
		incoming, err := batchConsumer.Receive(ctx)
		checked(err)
		var value *string
		checked(incoming.GetSchemaValue(&value))
		if value == nil || *value != expected {
			panic(fmt.Sprintf("batch message: got %v, want %q", value, expected))
		}
		checked(batchConsumer.Ack(incoming))
	}
	fmt.Println("MoonBit-to-Go batch decode OK")
}
