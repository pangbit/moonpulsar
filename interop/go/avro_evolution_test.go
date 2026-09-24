package main

import (
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
)

type oldEvolvingUser struct {
	ID   int32  `avro:"id"`
	Name string `avro:"name"`
}

type newEvolvingUser struct {
	Identifier int64  `avro:"identifier"`
	Name       string `avro:"name"`
	Active     bool   `avro:"active"`
}

// The pinned Go reader decodes Avro bytes with its configured schema instead
// of resolving the message's writer schema. Record this reference-client limit
// so the MoonBit v1-writer/v2-reader path is not mislabeled as Go interop.
func TestGoAvroReaderEvolutionBoundary(t *testing.T) {
	oldSchema, err := pulsar.NewAvroSchemaWithValidation(avroEvolutionWriter, nil)
	if err != nil {
		t.Fatal(err)
	}
	newSchema, err := pulsar.NewAvroSchemaWithValidation(`{"type":"record","name":"EvolvingUser","fields":[{"name":"identifier","type":"long","aliases":["id"]},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldBytes, err := oldSchema.Encode(oldEvolvingUser{ID: 19, Name: "old"})
	if err != nil {
		t.Fatal(err)
	}
	var newer newEvolvingUser
	err = newSchema.Decode(oldBytes, &newer)
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("expected pinned Go reader to reject absent default field, got %+v, %v", newer, err)
	}
	newBytes, err := newSchema.Encode(newEvolvingUser{Identifier: 23, Name: "new", Active: true})
	if err != nil {
		t.Fatal(err)
	}
	var older oldEvolvingUser
	if err := oldSchema.Decode(newBytes, &older); err != nil {
		t.Fatal(err)
	}
	if older.ID != 23 || older.Name != "new" {
		t.Fatalf("unexpected old-schema decode: %+v", older)
	}
}
