package signer

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	rd "math/rand"
)

type x509RSACertIssuor struct {
	root   RSACert
	priKey *rsa.PrivateKey
}

func Newx509RSACertIssuor(root RSACert, priKey *rsa.PrivateKey) RSACertIssuor {
	return &x509RSACertIssuor{
		root:   root,
		priKey: priKey,
	}
}

func (issuor *x509RSACertIssuor) GetRootCert() RSACert {
	return issuor.root
}

func (issuor *x509RSACertIssuor) Issue(subject *x509Subject) (RSACert, error) {
	cer := issuor.buildCertificate(subject)

	ca, err := x509.CreateCertificate(rand.Reader, cer, issuor.GetRootCert().GetCertificate(), &issuor.priKey.PublicKey, issuor.GetRootCert().GetPrivateKey())
	if err != nil {
		return nil, err
	}

	caPem := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ca,
	}
	ca = pem.EncodeToMemory(caPem)

	buf := x509.MarshalPKCS1PrivateKey(issuor.priKey)
	keyPem := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: buf,
	}
	key := pem.EncodeToMemory(keyPem)

	cert, err := Parsex509RSACert(ca, key)
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func (issuor *x509RSACertIssuor) buildCertificate(subject *x509Subject) *x509.Certificate {

	return &x509.Certificate{
		SerialNumber: big.NewInt(rd.Int63()), //证书序列号
		Subject: pkix.Name{
			Country:            subject.Country,
			Organization:       subject.Orianization,
			OrganizationalUnit: subject.OrianizationalUnit,
			Province:           subject.Province,
			CommonName:         subject.CommonName,
			Locality:           subject.Locality,
		},
		NotBefore:             subject.NotBefore, //证书有效期开始时间
		NotAfter:              subject.NotAfter,  //证书有效期结束时间
		BasicConstraintsValid: true,              //基本的有效性约束
		IsCA:        subject.IsRoot,      //是否是根证书
		ExtKeyUsage: subject.ExtKeyUsage, //证书用途(客户端认证，数据加密)
		KeyUsage:    subject.KeyUsage,
	}
}
