package worker_test

import (
	"context"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
)

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	conn := worker.NewConnection("https://sqs.us-east-1.amazonaws.com/88888888888/bucket")

	conn.Run(ctx,
		func(m *sqs.Message) error {
			log.Println(m)
			cancel()
			return nil
		})
}
