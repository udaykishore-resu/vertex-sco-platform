// Package identity provides mutual-TLS workload identity for service-to-
// service and terminal-to-store-to-cloud traffic, addressing "no explicit
// zero-trust/identity layer between tiers" from the architecture review.
//
// Design: every Vertex workload gets an X.509 certificate whose Subject
// Common Name encodes a SPIFFE-style identity URI
// (spiffe://vertex.local/<tier>/<service>), issued by a per-environment CA
// (see deploy/certs/generate-dev-ca.sh, which stands up a local CA and
// leaf certs for every service for docker-compose/dev use). In a real fleet
// this CA + rotation would be handled by an actual SPIFFE/SPIRE deployment;
// this package's LoadIdentity/ServerTLSConfig/ClientTLSConfig functions are
// written against a plain crypto/tls + crypto/x509 interface so swapping in
// SPIRE's workload API later only changes how the *cert bytes* are sourced,
// not how every service uses them.
package identity

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// SPIFFEID is a lightweight parsed form of the identity encoded in a
// certificate's CN, e.g. "spiffe://vertex.local/store/vertex-core".
type SPIFFEID struct {
	Trust   string // "vertex.local"
	Tier    string // "cloud" | "store" | "terminal"
	Service string // "vertex-core", etc.
}

func (id SPIFFEID) String() string {
	return fmt.Sprintf("spiffe://%s/%s/%s", id.Trust, id.Tier, id.Service)
}

// Identity bundles a workload's own cert/key with the CA pool it should
// trust for peer verification.
type Identity struct {
	ID      SPIFFEID
	CertPEM []byte
	KeyPEM  []byte
	CAPEM   []byte
}

// LoadFromFiles reads a workload's identity material from disk (mounted as
// a Kubernetes Secret / docker-compose bind mount in deploy/).
func LoadFromFiles(id SPIFFEID, certPath, keyPath, caPath string) (*Identity, error) {
	cert, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("identity: reading cert: %w", err)
	}
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("identity: reading key: %w", err)
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("identity: reading CA: %w", err)
	}
	return &Identity{ID: id, CertPEM: cert, KeyPEM: key, CAPEM: ca}, nil
}

// ServerTLSConfig builds a *tls.Config that requires and verifies client
// certificates (mTLS) — used by every service's HTTP/gRPC listener so a
// compromised or spoofed lane terminal cannot open a session without a
// valid workload cert from the Vertex CA.
func (i *Identity) ServerTLSConfig() (*tls.Config, error) {
	cert, err := tls.X509KeyPair(i.CertPEM, i.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("identity: loading keypair: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(i.CAPEM) {
		return nil, fmt.Errorf("identity: no CA certs found")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig builds a *tls.Config for calling another Vertex service,
// presenting this workload's own cert and verifying the peer against the
// shared CA — i.e. genuine mutual TLS, not just server-auth HTTPS.
func (i *Identity) ClientTLSConfig() (*tls.Config, error) {
	cert, err := tls.X509KeyPair(i.CertPEM, i.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("identity: loading keypair: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(i.CAPEM) {
		return nil, fmt.Errorf("identity: no CA certs found")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// PeerIdentity extracts the SPIFFE-style identity from a verified peer
// certificate's Common Name, for authorization decisions ("only
// spiffe://vertex.local/store/vertex-core may call vertex-intervention's
// /resolve endpoint").
func PeerIdentity(cert *x509.Certificate) string {
	return cert.Subject.CommonName
}
