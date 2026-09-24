import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

/** Official Java client probe for compressed chunk interoperability. */
public final class ChunkInterop {
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || (!args[0].equals("send") && !args[0].equals("receive"))) {
            throw new IllegalArgumentException("usage: ChunkInterop send|receive");
        }
        String base = System.getenv("PULSAR_TOPIC");
        String path = System.getenv("PULSAR_PAYLOAD_FILE");
        if (base == null || path == null) {
            throw new IllegalArgumentException("PULSAR_TOPIC and PULSAR_PAYLOAD_FILE are required");
        }
        byte[] payload = Files.readAllBytes(Paths.get(path));
        String url = System.getenv().getOrDefault("PULSAR_URL", "pulsar://127.0.0.1:6650");
        String[] names = {"lz4", "zlib", "zstd"};
        CompressionType[] codecs = {
            CompressionType.LZ4, CompressionType.ZLIB, CompressionType.ZSTD
        };
        try (PulsarClient client = PulsarClient.builder().serviceUrl(url).build()) {
            for (int i = 0; i < names.length; i++) {
                if (args[0].equals("send")) {
                    try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                            .topic(base + "-java-" + names[i])
                            .enableBatching(false)
                            .enableChunking(true)
                            .chunkMaxMessageSize(32768)
                            .compressionType(codecs[i]).create()) {
                        producer.send(payload);
                    }
                } else {
                    try (Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                            .topic(base + "-moonbit-" + names[i])
                            .subscriptionName("moonpulsar-chunk-java-" + names[i] + "-"
                                    + System.nanoTime())
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscribe()) {
                        Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
                        if (message == null || !Arrays.equals(message.getData(), payload)) {
                            throw new AssertionError(names[i] + ": chunk payload mismatch");
                        }
                        consumer.acknowledge(message);
                    }
                }
            }
        }
        System.out.println("Java " + args[0] + " compressed chunks: LZ4, Zlib, Zstd OK");
    }
}
