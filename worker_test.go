package worker_test

import (
	"context"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"testing"
)

type MockQueue struct {
	sqsiface.SQSAPI
	Result []*sqs.Message
}

type MockRequst interface {
	Send() error
}

type MockSender struct {
}

func (m *MockSender) Send() error { return nil }

func (m *MockQueue) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *MockQueue) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) (*request.Request, *sqs.ReceiveMessageOutput) {
	return &request.Request{}, &sqs.ReceiveMessageOutput{
		Messages: m.Result,
	}
}

func TestProcessMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := worker.NewConnection("https://sqs.us-east-1.amazonaws.com/88888888888/bucket")

	msgStr := "hello world"
	out := &sqs.Message{Body: &msgStr}
	test := []*sqs.Message{out}
	conn.Queue = &MockQueue{
		Result: test,
	}

	conn.Run(ctx,
		func(m *sqs.Message) error {
			if *m.Body != msgStr {
				t.Error("expected", msgStr, "got", m.Body)
			}
			cancel()
			return nil
		})
	return
}
