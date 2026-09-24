import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.DefaultCryptoKeyReader;

/** Official Java client probe for the P-521 ECIES wire format. */
public final class EncryptionInterop {
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || (!args[0].equals("send") && !args[0].equals("receive"))) {
            throw new IllegalArgumentException("usage: EncryptionInterop send|receive");
        }
        String topic = System.getenv("PULSAR_TOPIC");
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("PULSAR_TOPIC is required");
        }
        String url = System.getenv().getOrDefault("PULSAR_URL", "pulsar://127.0.0.1:6650");
        String publicFile = System.getenv("EC_PUBLIC_KEY");
        String privateFile = System.getenv("EC_PRIVATE_KEY");
        if (publicFile == null || privateFile == null) {
            throw new IllegalArgumentException("EC_PUBLIC_KEY and EC_PRIVATE_KEY are required");
        }
        DefaultCryptoKeyReader keys = DefaultCryptoKeyReader.builder()
                .publicKey("ec", publicFile).privateKey("ec", privateFile).build();
        ClientBuilder builder = PulsarClient.builder().serviceUrl(url)
                .operationTimeout(30, TimeUnit.SECONDS);
        String caFile = System.getenv("PULSAR_TLS_CA_FILE");
        if (caFile != null && !caFile.isEmpty()) {
            builder.tlsTrustCertsFilePath(caFile)
                    .allowTlsInsecureConnection(false)
                    .enableTlsHostnameVerification(true);
        }
        String clientCert = System.getenv("PULSAR_TLS_CLIENT_CERT_FILE");
        String clientKey = System.getenv("PULSAR_TLS_CLIENT_KEY_FILE");
        if (clientCert != null || clientKey != null) {
            if (clientCert == null || clientKey == null) {
                throw new IllegalArgumentException("both TLS client certificate and key are required");
            }
            builder.authentication(AuthenticationFactory.TLS(clientCert, clientKey));
        }
        try (PulsarClient client = builder.build()) {
            if (args[0].equals("send")) {
                try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                        .topic(topic + "-java").cryptoKeyReader(keys)
                        .addEncryptionKey("ec").enableBatching(false).create()) {
                    producer.send("from-java-ec".getBytes(StandardCharsets.UTF_8));
                }
                System.out.println("Java ECIES send OK");
            } else {
                try (Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topic(topic + "-moonbit")
                        .subscriptionName("moonpulsar-java-ec-" + System.nanoTime())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .cryptoKeyReader(keys).subscribe()) {
                    Message<byte[]> message = consumer.receive(20, TimeUnit.SECONDS);
                    if (message == null || !"from-moonbit-ec".equals(
                            new String(message.getData(), StandardCharsets.UTF_8))) {
                        throw new AssertionError("unexpected MoonBit ECIES payload");
                    }
                    consumer.acknowledge(message);
                }
                System.out.println("Java ECIES receive OK");
            }
        }
    }
}
