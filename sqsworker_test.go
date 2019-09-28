package sqsworker_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ajbeach2/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	Error   awserr.Error
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

func (m *MockQueue) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(fmt.Sprint("https://sqs.us-east-1.amazonaws.com/88888888888/", *input.QueueName)),
	}, m.Error
}

func (m *MockQueue) CreateQueue(input *sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error) {
	return &sqs.CreateQueueOutput{
		QueueUrl: aws.String(fmt.Sprint("https://sqs.us-east-1.amazonaws.com/88888888888/", *input.QueueName)),
	}, nil
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

func (t *MockTopic) CreateTopic(input *sns.CreateTopicInput) (*sns.CreateTopicOutput, error) {
	return &sns.CreateTopicOutput{
		TopicArn: aws.String(fmt.Sprint("arn:aws:sns:us-east-1:88888888888:", *input.Name)),
	}, nil
}

func GetMockTopic() *MockTopic {
	return &MockTopic{
		In:  make(chan string),
		Msg: "",
	}
}

func BenchmarkWorker(b *testing.B) {
	b.ReportAllocs()
	queue := GetMockeQueue()
	topic := GetMockTopic()

	var handlerFunction = func(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
		*w.Message = ""
		return nil
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		TopicArn: "arn:aws:sns:us-east-1:88888888888:Out",
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

	var handlerFunction = func(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
		w.Message = m.Body
		return errors.New("test error")
	}

	var callback = func(result *string, err error) {
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
	topic := GetMockTopic()
	done := make(chan bool)

	var handlerFunction = func(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
		*w.Message = fmt.Sprint(*m.Body, " ", "world")
		return nil
	}

	var callback = func(result *string, err error) {
		if *result != "hello world" {
			t.Error("Expected: ", "hello world", "Actual: ", result)
		}
		close(done)
	}

	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/88888888888/In",
		TopicArn: "arn:aws:sns:us-east-1:88888888888:Out",
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

func TestGetQueue(t *testing.T) {
	queue := GetMockeQueue()
	queueUrl, err := sqsworker.GetOrCreateQueue("In", queue)
	if queueUrl != "https://sqs.us-east-1.amazonaws.com/88888888888/In" {
		t.Error("Actual: ", queueUrl, "Expected: ", "https://sqs.us-east-1.amazonaws.com/88888888888/In")
	}

	if err != nil {
		t.Error(err)
	}
}

func TestCreateQueue(t *testing.T) {
	queue := GetMockeQueue()
	queue.Error = awserr.New(sqs.ErrCodeQueueDoesNotExist, "Queue Does Not Exists", nil)
	queueUrl, err := sqsworker.GetOrCreateQueue("In", queue)
	if queueUrl != "https://sqs.us-east-1.amazonaws.com/88888888888/In" {
		t.Error("Actual: ", queueUrl, "Expected: ", "https://sqs.us-east-1.amazonaws.com/88888888888/In")
	}

	if err != nil {
		t.Error(err)
	}
}

func TestGetOrCreateTopic(t *testing.T) {
	topic := GetMockTopic()
	topicArn, err := sqsworker.GetOrCreateTopic("Out", topic)
	if topicArn != "arn:aws:sns:us-east-1:88888888888:Out" {
		t.Error("Actual: ", topicArn, "Expected: ", "arn:aws:sns:us-east-1:88888888888:Out")
	}

	if err != nil {
		t.Error(err)
	}
}
