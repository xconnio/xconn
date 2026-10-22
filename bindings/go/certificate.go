package xconn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"time"
)

// GenerateSelfSignedTLSConfig creates a localhost development identity for WAMP-over-QUIC.
func GenerateSelfSignedTLSConfig() (*tls.Config, error) {
	config, _, err := GenerateWebTransportTLSConfig()
	if err != nil {
		return nil, err
	}

	config.NextProtos = []string{"wamp.2.quic"}
	return config, nil
}

// GenerateWebTransportTLSConfig creates a short-lived ECDSA identity suitable for
// browser serverCertificateHashes and returns its SHA-256 certificate fingerprint.
func GenerateWebTransportTLSConfig() (*tls.Config, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(13 * 24 * time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	hash := sha256.Sum256(certificate)
	return &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"},
		Certificates: []tls.Certificate{{Certificate: [][]byte{certificate}, PrivateKey: key}}}, hash[:], nil
}
