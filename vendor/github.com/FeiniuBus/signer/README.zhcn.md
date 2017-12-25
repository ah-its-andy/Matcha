[![Build Status](https://travis-ci.org/FeiniuBus/signer.svg?branch=master)](https://travis-ci.org/FeiniuBus/signer)

[English version](https://github.com/FeiniuBus/signer/blob/master/README.md)

# signer
高性能GO语言签名算法包。用于HTTP请求签名。

# X509 RSA Signature Server
使用RSA算法和x.509证书的签名服务

## 如何使用 ?
* Step 1 : 加载根证书
```
    root, err := Parsex509RSACert(**x.509格式的根证书**, **根证书的私钥**)
```

* Step 2 : 实例化一个存储实例，用于保存私钥等信息
** RSAStore 类型是线程安全的，因此你应当在一个程序边界内只实例化一个单例的对象 **
```
factory := NewRSAStoreFactory(** 标签（例如： dev） **, ** 亚马逊S3服务的存储桶名称 **, ** 第一步得到的根证书实例 **, ** 用于生成x.509证书的主体信息 (*signer.x509Subject) **)
store, err := factory.Create(x509RSAStore_OneToMany) //x509RSAStore_OneToMany: 这个枚举代表这个Store将使用一个私钥对应多个证书的模式 
```

* Step 3 : Create RSA Server and Client
```
server := Newx509RSAServer(** 第二步得到的Store实例 **)
client, err := server.CreateClient(** 客户端的唯一标识 **) //标识相同的客户端将使用相同的证书验证签名
```

* Step 4 : Sign
```
signature, key, err := client.Sign([]byte("testing")) 
//signature: 签名的计算结果，客户端需要使用证书验证这个值
//key: x.509 证书上传到亚马逊S3服务时使用的KEY
```

## 如何验签 ?
本服务使用的是 OpenSSL 签名方式， 绝大部分编程语言都支持此方式。

* 以下是一些相关参数 :
* 根证书密钥长度 : 2048
* 根证书密钥编码 : ASN.1
* 根证书格式 : x.509
* 根证书编码 : ASN.1
* 客户端证书密钥长度 : 2048
* 客户端证书密钥编码 : ASN.1
* 客户端证书格式 : x.509
* 客户端证书编码 : ASN.1

## 如何获得根证书 ?
* 可以在Linux运行以下命令生成OpenSSL根证书
```bash
openssl genrsa -out rsakey.pem 2048 && \
openssl rsa -in rsakey.pem -pubout -out rsakey.pub && \
openssl req -x509 -new -days 365 -key rsakey.pem -out rootcert.crt
```