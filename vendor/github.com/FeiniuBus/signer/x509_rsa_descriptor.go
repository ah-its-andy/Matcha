package signer

import (
	"crypto/rsa"
)

type x509RSADescriptor struct {
	clientID    string
	privateKey  *rsa.PrivateKey
	certificate string
}

func Newx509RSADescriptor(clientID string,
	certificate string,
	privateKey *rsa.PrivateKey) RSADescriptor {
	return &x509RSADescriptor{
		clientID:    clientID,
		privateKey:  privateKey,
		certificate: certificate,
	}
}

func (d *x509RSADescriptor) PrivateKey() *rsa.PrivateKey {
	return d.privateKey
}

func (d *x509RSADescriptor) Certificate() string {
	return d.certificate
}

func (d *x509RSADescriptor) ClientID() string {
	return d.clientID
}
