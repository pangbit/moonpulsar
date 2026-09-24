import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/** Official Java client probe for Avro writer/reader evolution across clients. */
public final class AvroEvolutionInterop {
    private static final String WRITER = "{\"type\":\"record\",\"name\":\"EvolvingUser\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final String READER = "{\"type\":\"record\",\"name\":\"EvolvingUser\",\"fields\":[{\"name\":\"identifier\",\"type\":\"long\",\"aliases\":[\"id\"]},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"active\",\"type\":\"boolean\",\"default\":true}]}";

    private static GenericSchema<GenericRecord> schema(String definition) {
        return Schema.generic(SchemaInfo.builder().name("EvolvingUser")
                .type(SchemaType.AVRO)
                .schema(definition.getBytes(StandardCharsets.UTF_8)).build());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || (!args[0].equals("send") && !args[0].equals("receive"))) {
            throw new IllegalArgumentException("usage: AvroEvolutionInterop send|receive");
        }
        String topic = System.getenv("PULSAR_TOPIC");
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("PULSAR_TOPIC is required");
        }
        String url = System.getenv().getOrDefault("PULSAR_URL", "pulsar://127.0.0.1:6650");
        try (PulsarClient client = PulsarClient.builder().serviceUrl(url)
                .operationTimeout(30, TimeUnit.SECONDS).build()) {
            if (args[0].equals("send")) {
                GenericSchema<GenericRecord> writer = schema(WRITER);
                GenericRecord record = writer.newRecordBuilder()
                        .set("id", 19).set("name", "from-java-v1").build();
                try (Producer<GenericRecord> producer = client.newProducer(writer)
                        .topic(topic + "-java-v1").enableBatching(false).create()) {
                    producer.send(record);
                }
                System.out.println("Java Avro v1 send OK");
            } else {
                try (Consumer<GenericRecord> consumer = client.newConsumer(schema(READER))
                        .topic(topic + "-moonbit-v1")
                        .subscriptionName("moonpulsar-java-avro-" + System.nanoTime())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> message = consumer.receive(20, TimeUnit.SECONDS);
                    if (message == null) {
                        throw new AssertionError("MoonBit Avro message not received");
                    }
                    GenericRecord value = message.getValue();
                    if (!Long.valueOf(23).equals(value.getField("identifier"))
                            || !"from-moonbit-v1".equals(value.getField("name"))
                            || !Boolean.TRUE.equals(value.getField("active"))) {
                        throw new AssertionError("unexpected evolved Java record: " + value);
                    }
                    consumer.acknowledge(message);
                }
                System.out.println("Java Avro v2 reader OK");
            }
        }
    }
}
