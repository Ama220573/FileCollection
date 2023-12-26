package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/crypto/ssh"
)

var sess *session.Session

type SSH struct {
	Host     string
	Port     string
	User     string
	Password string
}

func main() {

	createSSHConnection()

	// コマンド実行
	// var b bytes.Buffer
	// session.Stdout = &b
	// if err := session.Run("cd /work/keyDownloader/ && ls"); err != nil {
	// 	log.Fatal("Failed to run: " + err.Error())
	// }
	// fmt.Println(b.String())

	// コマンド実行
	// var b bytes.Buffer
	// session.Stdout = &b
	// if err := session.Run("cd /work/keyDownloader/ && ls"); err != nil {
	// 	log.Fatal("Failed to run: " + err.Error())
	// }
	// fmt.Println(b.String())

	// tmpKafkaPathList := GetKafkaKeyList()
	// ComapreLists(GetS3gzList("http://localhost:18333", "seaweedfsadmin", "seaweedfsadmin"), tmpKafkaPathList)
}

func createSSHConnection() {
	// SSH接続
	sshstagingConf := &SSH{
		Host:     "10.110.135.94",
		Port:     "22",
		User:     "root",
		Password: "TELTI$pr0ject",
	}

	sshkafkaConf := &SSH{
		Host:     "172.20.250.200",
		Port:     "22",
		User:     "root",
		Password: "TELTI$pr0ject",
	}

	// stagingへのConfig
	stagingConfig := &ssh.ClientConfig{
		User:            sshstagingConf.User,
		Auth:            []ssh.AuthMethod{ssh.Password(sshstagingConf.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// stagingへのssh Client
	stagingClient, err := ssh.Dial("tcp", net.JoinHostPort(sshstagingConf.Host, sshstagingConf.Port), stagingConfig)
	if err != nil {
		fmt.Println(err)
	}

	// kafka VMへのConfig
	kafkaConfig := &ssh.ClientConfig{
		User:            sshkafkaConf.User,
		Auth:            []ssh.AuthMethod{ssh.Password(sshkafkaConf.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// kafka VMへのssh Client
	kafkaClient, err := stagingClient.Dial("tcp", net.JoinHostPort(sshkafkaConf.Host, sshkafkaConf.Port))
	if err != nil {
		fmt.Println(err)
	}

	p2Connect, p2Chans, p2Reqs, err := ssh.NewClientConn(kafkaClient, net.JoinHostPort(sshkafkaConf.Host, sshkafkaConf.Port), kafkaConfig)
	if err != nil {
		fmt.Println(err)
	}

	client := ssh.NewClient(p2Connect, p2Chans, p2Reqs)

	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Failed to create session: ", err)
	}
	defer session.Close()

	tools := []string{
		"F00001",
		// "F00002",
	}

	for _, tool := range tools {
		command := "cd /work/keyDownloader/ && ./keyDownloader.sh " + tool + " > " + tool + ".csv && cat " + tool + ".csv"
		fmt.Println(command)

		// コマンド実行
		var b bytes.Buffer
		session.Stdout = &b
		if err := session.Run(command); err != nil {
			log.Fatal("Failed to run: " + err.Error())
		}
		// fmt.Println(b.String())

		// ディレクトリ作成
		if err := os.MkdirAll("CompareFiles", 0777); err != nil {
			fmt.Println(err)
		}

		// ファイル作成
		f, err := os.Create("CompareFiles/" + tool + ".csv")
		if err != nil {
			panic(err)
		}
		defer os.Remove(f.Name())

		if _, err = f.Write([]byte(b.Bytes())); err != nil {
			panic(err)
		}
	}

	// var s3List []string
	s3List := GetS3gzList("http://localhost:18333", "seaweedfsadmin", "seaweedfsadmin")
	kafkaKeyList := GetKafkaKeyList()

	ComapreLists(s3List, kafkaKeyList)
}

func ComapreLists(s3BucketList []string, kafkaPathList []string) {
	for _, gzfile := range s3BucketList {
		Contains(kafkaPathList, gzfile)
	}
}

func Contains(kafkaList []string, gzfile string) bool {
	for _, message := range kafkaList {
		if strings.Contains(message, gzfile) {
			return true
		}
	}
	fmt.Printf("Not found %s \n", gzfile)
	return false
}

func GetS3gzList(url string, user string, pass string) []string {
	var s3gzList []string
	gzFIleCount := 0
	sess = CreateS3Session(url, user, pass)
	for _, prefix := range []string{
		"DataPipeline/F00001/",
	} {
		items, _ := listObjects2(sess, "/eq-kafka", prefix)
		for _, item := range items {
			// fmt.Printf("%s\n", *item.Key)
			s3gzList = append(s3gzList, *item.Key)
			gzFIleCount++
		}
	}
	fmt.Printf("GZ file in S3 Bucket is %d \n", gzFIleCount)
	return s3gzList
}

func GetKafkaKeyList() []string {
	kafkaMessageCount := 0
	fileName := "CompareFiles/F00001.csv"
	fp, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	reader := csv.NewReader(fp)
	//reader.Comma = ','
	reader.LazyQuotes = true
	var record []string
	var tmp []string
	for {
		tmp, err = reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		record = append(record, tmp[0])
		kafkaMessageCount++
	}
	fmt.Printf("kafka Message in Topic is %d \n", kafkaMessageCount)
	return record
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
