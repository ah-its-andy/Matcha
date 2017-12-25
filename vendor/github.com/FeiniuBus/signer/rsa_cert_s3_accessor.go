package signer

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type RSACertS3Accessor struct {
	Region  string
	Bucket  string
	Key     string
	Profile string
}

//ParseS3URI sample : s3://default/sampleBucket/?key=sampleKey&profile=Profile1 .
func ParseS3URI(uri string) (RSACertAccessor, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("'s3://' is expected, but the '%s://' is provided", u.Scheme)
	}
	// if strings.Index("/", u.Path) == -1 || len(strings.Split("/", u.Path)) != 2 {
	// 	return nil, fmt.Errorf("path '%s' format is incorrect, should be '{Bucket}/{Key}'", u.Path)
	// }
	// path := strings.Split("/", u.Path)
	r := &RSACertS3Accessor{
		Bucket: u.Path,
	}

	if u.Host != "default" {
		r.Region = u.Host
	}

	if u.RawQuery == "" {
		return nil, errors.New("query arguments could not be empty")
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	key, ok := m["key"]
	if ok == false {
		return nil, errors.New("query argument 'key' was required")
	}
	r.Key = key[0]

	profile, ok := m["profile"]
	if ok {
		r.Profile = profile[0]
	}

	return r, nil
}

func (u *RSACertS3Accessor) Session() *session.Session {
	var sess *session.Session
	var options session.Options

	if u.Profile != "" {
		options.Profile = u.Profile
	}

	config := aws.NewConfig()
	config = config.WithRegion(u.Region)
	options.Config = *config

	sess = session.Must(session.NewSessionWithOptions(options))

	return sess
}

func (u *RSACertS3Accessor) Upload(body []byte) error {
	sess := u.Session()
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(u.Bucket),
		Key:    aws.String(u.Key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return err
	}
	return nil
}

func (u *RSACertS3Accessor) Download() ([]byte, error) {
	sess := u.Session()
	downloader := s3manager.NewDownloader(sess)
	buffer := new(aws.WriteAtBuffer)
	_, err := downloader.Download(buffer, &s3.GetObjectInput{
		Bucket: aws.String(u.Bucket),
		Key:    aws.String(u.Key),
	})
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
