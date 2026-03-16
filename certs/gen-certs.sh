#!/usr/bin/env sh
# Generate self-signed test certificates for local TLS testing.
#
# Produces in this directory:
#   ca.pem          CA certificate (trust anchor)
#   server.pem      Mosquitto server certificate
#   server.key      Mosquitto server private key
#   client.pem      Client certificate (used by tls.pl)
#   client.key      Client private key (used by tls.pl)
#
# These are for local testing only — not for production use.

set -e
cd "$(dirname "$0")"

# CA
openssl genrsa -out ca.key 2048
openssl req -x509 -new -key ca.key -days 3650 -out ca.pem \
    -subj "/CN=test-ca"

# Server (Mosquitto)
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/CN=mosquitto"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -days 3650 -out server.pem

# Client (tls.pl)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/CN=test-client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -days 3650 -out client.pem

rm -f ca.key ca.srl server.csr client.csr

# make all files world-readable so container processes (e.g. mosquitto)
# can read them regardless of which uid owns the volume mount
chmod 644 ca.pem server.pem server.key client.pem client.key

echo "certs generated in $(pwd)"
