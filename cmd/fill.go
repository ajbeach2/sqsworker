package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"os"
)

func main() {
	var sess = session.New(&aws.Config{Region: aws.String("us-east-1")})
	q := sqs.New(sess)
	queueurl := os.Getenv("QUEUE")
	params := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueurl),
		MessageBody: aws.String("Hello world"),
	}

	for {
		q.SendMessage(params)
	}
}
