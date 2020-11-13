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

type LowerCaseWorker struct {
}

func (l *LowerCaseWorker) Process(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
	*w.Message = strings.ToLower(*m.Body)
	return nil
}

func ExampleWorker() {

	lowerCaseWorker := &LowerCaseWorker{}
	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

	queueURL, _ := sqsworker.GetOrCreateQueue("In", sqs.New(sess))
	topicArn, _ := sqsworker.GetOrCreateTopic("Out", sns.New(sess))

	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueURL:  queueURL,
		TopicArn:  topicArn,
		Workers:   1,
		Processor: lowerCaseWorker,
		Name:      "TestApp",
	})

	w.Run()
}
