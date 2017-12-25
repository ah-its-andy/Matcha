package signer

type RSACertAccessor interface {
	Upload(body []byte) error
	Download() ([]byte, error)
}
