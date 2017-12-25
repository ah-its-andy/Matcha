package signer

import "crypto/rsa"

type RSASigner interface {
	Sign(bytes []byte, key *rsa.PrivateKey) ([]byte, error)
}
