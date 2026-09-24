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
		defer producer.Close()
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{Value: message(descriptor, "go")})
		checked(err)
		fmt.Println("Go Protobuf Native send OK")
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
			return
		}
		checked(consumer.Ack(incoming))
	}
}
