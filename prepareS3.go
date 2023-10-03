package main

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const uploadFilePath = "data/result/"
const awsRegion = "eu-north-1"
const outputBucket = "outputyoutubebucket"
const inputBucket = "inputyoutubebucket"

var countries = [11]string{"BR", "CA", "RU", "GB", "KR", "JP", "DE", "MX", "IN", "US", "FR"}

func main() {
	fmt.Println(os.Args)
	switch os.Args[1] {
	case "uploadInput":
		{
			if len(os.Args) < 3 {
				return
			}
			err2 := unzipSource("data/"+os.Args[2], "data/result")
			if err2 != nil {
				fmt.Println(err2)
			}
			deleteAllFilesInS3Location(inputBucket, "input")
			uploadFiles()
		}
	case "clearResult":
		clearResultFiles()
	}
}

func createS3Session() (*s3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		fmt.Println("Failed to create AWS session:", err)
		return nil, errors.New("Failed to create AWS session")
	}

	// Create an S3 service client
	return s3.New(sess), nil
}

func clearResultFiles() {
	for _, country := range countries {
		deleteAllFilesInS3Location(outputBucket, "awsresult/"+country)
		deleteAllFilesInS3Location(outputBucket, "azureresult/"+country)
	}
}

func deleteAllFilesInS3Location(bucketName, s3Path string) {

	svc, err := createS3Session()
	if err != nil {
		return
	}
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(s3Path),
	}

	resp, _ := svc.ListObjects(params)

	for _, obj := range resp.Contents {
		fmt.Println("Deleting:", *obj.Key)
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			fmt.Println("Error deleting object:", err)
		}
	}
}

func uploadFiles() {

	entries, err := os.ReadDir("data/result/")
	if err != nil {
		log.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	for _, currentFile := range entries {
		wg.Add(1)
		svc, err := createS3Session()
		if err != nil {
			return
		}
		go func(currentFile os.DirEntry) {
			defer wg.Done()
			file, err := os.Open(uploadFilePath + currentFile.Name())
			if err != nil {
				fmt.Println("Error opening file:", err)
				return
			}
			defer file.Close()
			fmt.Println(file.Name())

			s3ObjectKey := "input/" + currentFile.Name()
			fmt.Println(s3ObjectKey)
			_, err = svc.PutObject(&s3.PutObjectInput{
				Bucket: aws.String("inputyoutubebucket"),
				Key:    aws.String(s3ObjectKey),
				Body:   file,
			})

			if err != nil {
				fmt.Println("Error uploading file to S3:", err)
				return
			}
		}(currentFile)

	}

	wg.Wait()
}
