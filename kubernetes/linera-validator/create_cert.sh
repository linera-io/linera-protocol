 openssl req -x509 \
  -newkey rsa:4096 \
  -nodes \
  -keyout key.pem \
  -out cert.pem \
  -days 365 \
  -addext "subjectAltName = DNS:scylla-operator-webhook.scylla-operator.svc"
