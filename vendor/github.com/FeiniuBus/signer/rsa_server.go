package signer

type RSAServer interface {
	CreateClient(clientID string) (RSAClient, error)
}
