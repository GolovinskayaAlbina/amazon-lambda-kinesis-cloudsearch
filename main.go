package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudsearchdomain"
)

type KinesisEventData struct {
	FilePath string `json:"filePath"`
	Id       int    `json:"id"`
}

type CloudSearchDocument struct {
	Directory     string `json:"dir"`
	FileName      string `json:"name"`
	FileExtension string `json:"ext"`
}

type AmazonDocumentUploadRequest struct {
	Type   string              `json:"type"`
	Id     string              `json:"id"`
	Fields CloudSearchDocument `json:"fields"`
}

type Response struct {
	Ok bool `json:"ok"`
}

func Handler(ctx context.Context, kinesisEvent events.KinesisEvent) (Response, error) {
	fmt.Println("Get request from kinesis")
	var amasonDocumentsBatch []AmazonDocumentUploadRequest
	//Preparing data
	for _, record := range kinesisEvent.Records {
		kinesisRecord := record.Kinesis
		dataBytes := kinesisRecord.Data
		fmt.Printf("%s Data = %s \n", record.EventName, string(dataBytes))

		//Deserialize data from kinesis to KinesisEventData
		var eventData KinesisEventData
		err := json.Unmarshal(dataBytes, &eventData)
		if err != nil {
			return failed(), err
		}

		//Convert data to CloudSearch format
		document := ConvertToCloudSearchDocument(eventData)
		request := CreateAmazonDocumentUploadRequest(eventData.Id, document)
		amasonDocumentsBatch = append(amasonDocumentsBatch, request)
	}

	if len(amasonDocumentsBatch) > 0 {
		fmt.Print("Connecting to cloudsearch...\n")
		svc := cloudsearchdomain.New(session.New(&aws.Config{
			Region:     aws.String(os.Getenv("SearchRegion")),
			Endpoint:   aws.String(os.Getenv("SearchEndpoint")),
			MaxRetries: aws.Int(6),
		}))
		fmt.Print("Creating request...\n")

		batch, err := json.Marshal(amasonDocumentsBatch)
		if err != nil {
			return failed(), err
		}
		fmt.Printf("Search document = %s \n", batch)

		params := &cloudsearchdomain.UploadDocumentsInput{
			ContentType: aws.String("application/json"),
			Documents:   strings.NewReader(string(batch)),
		}
		fmt.Print("Starting to upload...\n")
		req, resp := svc.UploadDocumentsRequest(params)
		fmt.Print("Send request...\n")
		err = req.Send()
		if err != nil {
			return failed(), err
		}
		fmt.Println(resp)
	}
	return success(), nil
}

func CreateAmazonDocumentUploadRequest(id int, document CloudSearchDocument) AmazonDocumentUploadRequest {
	return AmazonDocumentUploadRequest{
		Type:   "add",
		Id:     strconv.Itoa(id),
		Fields: document,
	}
}

func ConvertToCloudSearchDocument(data KinesisEventData) CloudSearchDocument {

	var ext string
	extWithDot := filepath.Ext(data.FilePath)
	if extWithDot != "" {
		ext = extWithDot[1:len(extWithDot)]
	}
	dir, file := filepath.Split(strings.Replace(data.FilePath, "\\", "/", -1))
	return CloudSearchDocument{
		FileExtension: ext,
		Directory:     dir,
		FileName:      file,
	}
}

func failed() Response {
	return Response{
		Ok: false,
	}
}

func success() Response {
	return Response{
		Ok: true,
	}
}

func main() {
	lambda.Start(Handler)
}
