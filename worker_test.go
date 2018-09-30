package worker_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ajbeach2/worker"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"testing"
	"time"
)

type MockQueue struct {
	sqsiface.SQSAPI
	In  chan string
	Out chan string
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

	w := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
		Name:     "TestApp",
	})
	w.Queue = queue
	go func() {
		for i := 0; i < b.N; i++ {
			queue.Push("test")
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
		if _, ok := err.(*worker.HandlerTimeout); !ok {
			t.Error("expected worker.HandlerTimeout error")
		}
	}

	w := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
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

	w := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		Workers:  1,
		Region:   "us-east-1",
		Handler:  handlerFunction,
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

	w := worker.NewWorker(worker.WorkerConfig{
		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
		Workers:  1,
		Region:   "us-east-1",
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
