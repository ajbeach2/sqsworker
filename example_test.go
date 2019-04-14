package sqsworker_test

import (
	"context"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

func ExampleWorker() {
	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte(strings.ToLower(*m.Body)), nil
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

	w := worker.NewWorker(sess, worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Handler:  handlerFunction,
		Name:     "TestApp",
	})

	w.Run()
}
