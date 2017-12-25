package signer

type RSACertIssuor interface {
	GetRootCert() RSACert
	Issue(subject *x509Subject) (RSACert, error)
}
