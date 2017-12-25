package signer

import (
	"crypto/rsa"
	"crypto/x509"
	"math/big"
)

type RSACert interface {
	GetSerialNumber() *big.Int
	GetCertificate() *x509.Certificate
	GetPrivateKey() *rsa.PrivateKey

	GetCertificateBytes() []byte
	GetPrivateKeyBytes() []byte
}
