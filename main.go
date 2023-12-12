package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var sess *session.Session

func main() {
	sess = CreateS3Session("http://localhost:18333", "seaweedfsadmin", "seaweedfsadmin")
	for _, prefix := range []string{
		"DataPipeline/F00001/",
	} {
		items, _ := listObjects2(sess, "/eq-kafka", prefix)
		for _, item := range items {
			fmt.Printf("%s\n", *item.Key)
		}
	}
}

func CreateS3Session(url string, id string, secret string) *session.Session {
	myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.S3ServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           url,
				SigningRegion: "custom-signing-region",
			}, nil
		}

		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}
	creds := credentials.NewStaticCredentials(id, secret, "")
	sess := session.Must(session.NewSession(&aws.Config{
		EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
		Credentials:      creds,
	}))

	return sess
}

func listObjects2(sess *session.Session, backetName string, prefix string) ([]*s3.Object, error) {
	ret := []*s3.Object{}
	s3Srv := s3.New(sess)
	truncatedListing := true
	params := &s3.ListObjectsV2Input{Bucket: aws.String(backetName), Prefix: aws.String(prefix), MaxKeys: aws.Int64(10000)}
	for truncatedListing {
		resp, err := s3Srv.ListObjectsV2(params)

		if err != nil {
			fmt.Println(err.Error())
			return ret, err
		}
		for _, cnt := range resp.Contents {
			if !strings.HasSuffix(*cnt.Key, "/") {
				ret = append(ret, cnt)
			}
		}
		params.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}
	return ret, nil
}
