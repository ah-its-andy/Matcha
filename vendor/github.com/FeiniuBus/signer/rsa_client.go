package signer

type RSAClient interface {
	Sign(input []byte) ([]byte, string, error)
}
