package sqsworker_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ajbeach2/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"testing"
)

type MockQueue struct {
	sqsiface.SQSAPI
	In      chan string
	Out     chan string
	req     *request.Request
	receive *sqs.ReceiveMessageOutput
	Msg     string
}

func (m *MockQueue) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return nil, nil
}

func (m *MockQueue) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) (*request.Request, *sqs.ReceiveMessageOutput) {
	m.Msg, _ = <-m.In

	m.receive.Messages[0].Body = &m.Msg

	return m.req, m.receive
}

func (m *MockQueue) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.Out <- *input.MessageBody
	return nil, nil
}

func (m *MockQueue) Push(msg string) {
	m.In <- msg
}

func (m *MockQueue) Pop() string {
	return <-m.Out
}

func (m *MockQueue) Close() {
	close(m.In)
	close(m.Out)
}

func GetMockeQueue() *MockQueue {
	return &MockQueue{
		In:  make(chan string),
		Out: make(chan string),
		req: &request.Request{},
		receive: &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{Body: nil}},
		},
		Msg: "",
	}
}

type MockTopic struct {
	snsiface.SNSAPI
	In  chan string
	Msg string
}

func (t *MockTopic) Close() {
	close(t.In)
}

func (t *MockTopic) Publish(input *sns.PublishInput) (*sns.PublishOutput, error) {
	t.In <- *input.Message
	return nil, nil
}

func (t *MockTopic) GetMessage() *string {
	t.Msg, _ = <-t.In
	return &t.Msg
}

func GetMockeTopic() *MockTopic {
	return &MockTopic{
		In:  make(chan string),
		Msg: "",
	}
}

func BenchmarkWorker(b *testing.B) {
	b.ReportAllocs()
	queue := GetMockeQueue()
	topic := GetMockeTopic()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte{}, nil
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		TopicArn: "arn:aws:sns:us-east-1:88888888888:out",
		Workers:  1,
		Logger:   zap.NewNop(),
		Handler:  handlerFunction,
		Name:     "TestApp",
	})
	w.Queue = queue
	w.Topic = topic
	go func() {
		for i := 0; i < b.N; i++ {
			queue.Push("")
			topic.GetMessage()
		}
		w.Close()
	}()

	w.Run()
	queue.Close()
	topic.Close()
}

func TestError(t *testing.T) {
	queue := GetMockeQueue()
	done := make(chan bool)

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte(*m.Body), errors.New("test error")
	}

	var callback = func(result []byte, err error) {
		if err == nil {
			t.Error("Expected error")
		}
		done <- true
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		Workers:  1,
		Handler:  handlerFunction,
		Logger:   zap.NewNop(),
		Callback: callback,
		Name:     "TestApp",
	})
	w.Queue = queue

	go func() {
		queue.Push("hello") // No message are sent on error, so no pop needed
		<-done
		w.Close()
	}()

	w.Run()
	queue.Close()
	return
}

func TestProcessMessage(t *testing.T) {
	queue := GetMockeQueue()
	topic := GetMockeTopic()
	done := make(chan bool)

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		transform := fmt.Sprint(*m.Body, " ", "world")
		return []byte(transform), nil
	}

	var callback = func(result []byte, err error) {
		if string(result) != "hello world" {
			t.Error("Expected: ", "hello world", "Actual: ", string(result))
		}
		close(done)
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		TopicArn: "arn:aws:sns:us-east-1:88888888888:out",
		Workers:  1,
		Logger:   zap.NewNop(),
		Handler:  handlerFunction,
		Callback: callback,
		Name:     "TestApp",
	})
	w.Queue = queue
	w.Topic = topic

	go func() {
		queue.Push("hello")
		out := topic.GetMessage()

		if *out != "hello world" {
			t.Error("topic message does not match \"hello world\"", *out)
		}

		<-done
		w.Close()
	}()

	w.Run()
	queue.Close()
	topic.Close()
	return
}
