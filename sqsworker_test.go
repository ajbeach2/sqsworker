package sqsworker_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ajbeach2/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"testing"
	"time"
)

type MockQueue struct {
	sqsiface.SQSAPI
	In      chan string
	Out     chan string
	req     *request.Request
	recieve *sqs.ReceiveMessageOutput
}

var Msg string

func (m *MockQueue) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return nil, nil
}

func (m *MockQueue) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) (*request.Request, *sqs.ReceiveMessageOutput) {
	Msg = <-m.In
	m.recieve.Messages[0].Body = &Msg
	return m.req, m.recieve
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
		recieve: &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{Body: nil}},
		},
	}
}

func BenchmarkWorker(b *testing.B) {
	b.ReportAllocs()
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte{}, nil
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Logger:   zap.NewNop(),
		Handler:  handlerFunction,
		Name:     "TestApp",
	})
	w.Queue = queue
	go func() {
		for i := 0; i < b.N; i++ {
			queue.Push("")
			queue.Pop()
		}
		w.Close()
	}()

	w.Run()
	queue.Close()
}

func TestTimeout(t *testing.T) {
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		time.Sleep(3 * time.Second)
		return []byte(*m.Body), nil
	}

	var callback = func(result []byte, err error) {
		if _, ok := err.(*sqsworker.HandlerTimeoutError); !ok {
			t.Error("expected worker.HandlerTimeoutError error")
		}
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		Workers:  1,
		Handler:  handlerFunction,
		Logger:   zap.NewNop(),
		Callback: callback,
		Name:     "TestApp",
		Timeout:  1,
	})
	w.Queue = queue

	go func() {
		queue.Push("hello")
		time.Sleep(1 * time.Second)
		w.Close()
	}()

	w.Run()
	queue.Close()
	return
}

func TestError(t *testing.T) {
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte(*m.Body), errors.New("test error")
	}

	var callback = func(result []byte, err error) {
		if err == nil {
			t.Error("Expected error")
		}
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		Workers:  1,
		Handler:  handlerFunction,
		Logger:   zap.NewNop(),
		Callback: callback,
		Name:     "TestApp",
	})
	w.Queue = queue

	go func() {
		queue.Push("hello")
		w.Close()
	}()

	w.Run()
	queue.Close()
	return
}

func TestProcessMessage(t *testing.T) {
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		transform := fmt.Sprint(*m.Body, " ", "world")
		return []byte(transform), nil
	}

	var callback = func(result []byte, err error) {
		if string(result) != "hello world" {
			t.Error("Expected: ", "hello world", "Actual: ", string(result))
		}
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Logger:   zap.NewNop(),
		Handler:  handlerFunction,
		Callback: callback,
		Name:     "TestApp",
	})
	w.Queue = queue

	go func() {
		queue.Push("hello")
		queue.Pop()
		w.Close()
	}()

	w.Run()
	queue.Close()
	return
}
