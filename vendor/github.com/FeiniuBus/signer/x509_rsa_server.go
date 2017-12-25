package signer

type x509RSAServer struct {
	store RSAStore
}

func Newx509RSAServer(store RSAStore) RSAServer {
	return &x509RSAServer{
		store: store,
	}
}

func (s *x509RSAServer) CreateClient(clientID string) (RSAClient, error) {
	descriptor, err := s.store.Certificate(clientID)
	if err != nil {
		return nil, err
	}
	return &x509RSAClient{
		server:     s,
		descriptor: descriptor,
		clientID:   clientID,
	}, nil
}
