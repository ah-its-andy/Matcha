package signer

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
)

type x509RSASigner struct {
}

func Newx509RSASigner() RSASigner {
	return &x509RSASigner{}
}

func (signer *x509RSASigner) Sign(bytes []byte, key *rsa.PrivateKey) ([]byte, error) {
	cipher := sha256.New()
	cipher.Write(bytes)
	hashed := cipher.Sum(nil)
	signature, err := rsa.SignPKCS1v15(nil, key, crypto.SHA256, hashed)
	if err != nil {
		return nil, err
	}
	return signature, err
}
