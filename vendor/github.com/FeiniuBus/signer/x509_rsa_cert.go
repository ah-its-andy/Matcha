package signer

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"math/big"
)

type x509RSACert struct {
	serialNumber *big.Int
	certificate  *x509.Certificate
	privateKey   *rsa.PrivateKey

	certificateBytes []byte
	privateKeyBytes  []byte
}

func Parsex509RSACert(certificateBytes []byte, privateKeyBytes []byte) (RSACert, error) {
	certificate, err := ParseX509Certificate(certificateBytes)
	if err != nil {
		return nil, err
	}
	privateKey, err := ParseRsaPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, err
	}
	return &x509RSACert{
		serialNumber:     certificate.SerialNumber,
		certificate:      certificate,
		privateKey:       privateKey,
		certificateBytes: certificateBytes,
		privateKeyBytes:  privateKeyBytes,
	}, nil
}

func ParseX509Certificate(bytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(bytes)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func ParseRsaPrivateKey(bytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(bytes)
	pri, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return pri, nil
}

func (cert *x509RSACert) GetSerialNumber() *big.Int {
	return cert.serialNumber
}
func (cert *x509RSACert) GetCertificate() *x509.Certificate {
	return cert.certificate
}
func (cert *x509RSACert) GetPrivateKey() *rsa.PrivateKey {
	return cert.privateKey
}
func (cert *x509RSACert) GetCertificateBytes() []byte {
	return cert.certificateBytes
}
func (cert *x509RSACert) GetPrivateKeyBytes() []byte {
	return cert.privateKeyBytes
}
