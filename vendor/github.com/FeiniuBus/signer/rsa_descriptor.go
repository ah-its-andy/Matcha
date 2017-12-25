package signer

import (
	"crypto/rsa"
)

type RSADescriptor interface {
	PrivateKey() *rsa.PrivateKey
	Certificate() string
	ClientID() string
}
