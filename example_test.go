package worker_test

import (
	"context"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

func Example() {
	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte(strings.ToLower(*m.Body)), nil
	}

	conn := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
		Name:     "TestApp",
	})

	conn.Run()
}
