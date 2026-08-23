#!/usr/bin/env bash
# Generates a local development CA and one leaf certificate per Vertex
# service, for docker-compose / local mTLS testing (see internal/identity).
# NOT for production use — a real deployment issues these via SPIRE or an
# internal PKI (e.g. HashiCorp Vault, AWS Private CA) with short-lived certs
# and automatic rotation.
set -euo pipefail
cd "$(dirname "$0")"

TRUST_DOMAIN="vertex.local"
DAYS=825

SERVICES=(
  "cloud:vertex-control-plane"
  "cloud:vertex-config"
  "store:vertex-core"
  "store:vertex-intervention"
  "store:vertex-weight"
  "store:vertex-coupon"
  "store:vertex-visualverify"
  "store:vertex-trilight"
  "store:vertex-picklist"
  "store:vertex-cash"
  "store:vertex-doc"
  "store:vertex-print"
  "store:vertex-weightlearning"
  "store:vertex-errorlookup"
  "store:vertex-auth"
  "store:vertex-resources"
  "store:vertex-inputsequencer"
  "store:vertex-pos-bridge"
  "store:vertex-posless-adapter"
  "store:vertex-outbox"
  "store:vertex-agent"
  "terminal:vertex-endpoint"
  "terminal:vertex-devicegateway"
  "terminal:vertex-launchpad"
)

mkdir -p ca
if [ ! -f ca/ca.key ]; then
  echo "==> generating root CA for ${TRUST_DOMAIN}"
  openssl ecparam -name prime256v1 -genkey -noout -out ca/ca.key
  openssl req -x509 -new -nodes -key ca/ca.key -sha256 -days $((DAYS*3)) \
    -subj "/O=Vertex SCO Platform/CN=vertex-root-ca" \
    -out ca/ca.crt
fi

for entry in "${SERVICES[@]}"; do
  tier="${entry%%:*}"
  svc="${entry##*:}"
  outdir="issued/${svc}"
  mkdir -p "$outdir"
  cn="spiffe://${TRUST_DOMAIN}/${tier}/${svc}"
  cn_escaped="spiffe:\/\/${TRUST_DOMAIN}\/${tier}\/${svc}"
  echo "==> issuing cert for ${cn}"
  openssl ecparam -name prime256v1 -genkey -noout -out "${outdir}/tls.key"
  openssl req -new -key "${outdir}/tls.key" -subj "/O=Vertex SCO Platform/CN=${cn_escaped}" \
    -out "${outdir}/tls.csr"
  openssl x509 -req -in "${outdir}/tls.csr" -CA ca/ca.crt -CAkey ca/ca.key \
    -CAcreateserial -days "${DAYS}" -sha256 -out "${outdir}/tls.crt"
  cp ca/ca.crt "${outdir}/ca.crt"
  rm -f "${outdir}/tls.csr"
done

echo "==> done. Certs under deploy/certs/issued/<service>/{tls.crt,tls.key,ca.crt}"
