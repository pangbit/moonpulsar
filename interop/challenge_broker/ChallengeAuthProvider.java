package moonpulsar.test;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/** Disposable provider that requires a Broker-issued binary auth challenge. */
public final class ChallengeAuthProvider implements AuthenticationProvider {
    @Override
    public void initialize(ServiceConfiguration config) {}

    @Override
    public String getAuthMethodName() {
        return "challenge-test";
    }

    @Override
    public AuthenticationState newAuthState(AuthData initial, SocketAddress address, SSLSession session) {
        return new AuthenticationState() {
            private int step;

            @Override
            public AuthData authenticate(AuthData data) throws AuthenticationException {
                if (!"hello".equals(new String(data.getBytes(), StandardCharsets.UTF_8))) {
                    throw new AuthenticationException("wrong challenge response");
                }
                if (step++ == 0) {
                    return AuthData.of("reply-required".getBytes(StandardCharsets.UTF_8));
                }
                return null;
            }

            @Override
            public String getAuthRole() {
                return "moonpulsar-challenge";
            }

            @Override
            public AuthenticationDataSource getAuthDataSource() {
                return new AuthenticationDataCommand("hello", address, session);
            }

            @Override
            public boolean isComplete() {
                return step >= 2;
            }
        };
    }

    @Override
    public void close() throws IOException {}
}
