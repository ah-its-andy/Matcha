package signer

type RSAStore interface {
	SetTag(tag string)
	Tag() string

	Certificate(clientID string) (RSADescriptor, error)
}
