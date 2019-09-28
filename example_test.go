package sqsworker_test

import (
	"context"
	"github.com/ajbeach2/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

func ExampleWorker() {
	var handlerFunction = func(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
		*w.Message = strings.ToLower(*m.Body)
		return nil
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

	queueUrl, _ := sqsworker.GetOrCreateQueue("In", sess)
	topicArn, _ := sqsworker.GetOrCreateTopic("Out", sess)

	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: queueUrl,
		TopicArn: topicArn,
		Workers:  1,
		Handler:  handlerFunction,
		Name:     "TestApp",
	})

	w.Run()
}
