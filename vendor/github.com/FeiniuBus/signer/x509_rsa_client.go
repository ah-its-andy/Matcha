package signer

type x509RSAClient struct {
	server     RSAServer
	descriptor RSADescriptor
	clientID   string
}

func (p *x509RSAClient) Sign(input []byte) ([]byte, string, error) {
	signer := Newx509RSASigner()
	signature, err := signer.Sign(input, p.descriptor.PrivateKey())
	if err != nil {
		return nil, "", err
	}
	return signature, p.descriptor.Certificate(), nil
}
