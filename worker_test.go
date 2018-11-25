package worker_test

import (
	"context"
	"fmt"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"testing"
)

type MockQueue struct {
	sqsiface.SQSAPI
	Result []*sqs.Message
	In     chan string
	Out    chan string
}

func (m *MockQueue) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *MockQueue) ReceiveMessageRequest(input *sqs.ReceiveMessageInput) (*request.Request, *sqs.ReceiveMessageOutput) {
	msg := <-m.In
	out := &sqs.Message{Body: &msg}
	return &request.Request{}, &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{out},
	}
}

func (m *MockQueue) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.Out <- *input.MessageBody
	return &sqs.SendMessageOutput{}, nil
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
	}
}

func BenchmarkHello(b *testing.B) {
	b.ReportAllocs()
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		return []byte(*m.Body), nil
	}

	conn := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
		Name:     "TestApp",
	})
	conn.Queue = queue
	go func() {
		for i := 0; i < b.N; i++ {
			queue.Push("test")
			queue.Pop()
		}
		conn.Close()
	}()

	conn.Run()
	queue.Close()
}

func TestProcessMessage(t *testing.T) {
	queue := GetMockeQueue()

	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
		transform := fmt.Sprint(*m.Body, " ", "world")
		return []byte(transform), nil
	}

	conn := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
		Name:     "TestApp",
	})
	conn.Queue = queue

	go func() {
		queue.Push("hello")
		result := queue.Pop()
		if result != "hello world" {
			t.Error("Actual", result, "Expected", "hello world")
		}
		conn.Close()
	}()
	conn.Run()
	queue.Close()
	return
}
