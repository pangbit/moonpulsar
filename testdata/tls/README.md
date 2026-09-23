`server.crt` and `server.key` are a self-signed `CN=localhost` pair used only by
the in-process TLS mock tests. The private key is intentionally public; do not
use this pair for any real broker or service. `.moonignore` excludes these test
fixtures from the Mooncakes release archive.
