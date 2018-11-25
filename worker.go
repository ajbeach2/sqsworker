package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Handler func(context.Context, *sqs.Message) ([]byte, error)

type Worker struct {
	QueueInUrl  string
	QueueOutUrl string
	Queue       sqsiface.SQSAPI
	Session     *session.Session
	Consumers   int
	Logger      *zap.Logger
	Handler     Handler
	Name        string
	done        chan error
}

type WorkerConfig struct {
	QueueIn  string
	QueueOut string
	Workers  int
	Region   string
	Handler  Handler
	Name     string
}

type consumerDone struct {
	Result []byte
	Err    error
}

func (conn *Worker) LogError(err error) {
	conn.Logger.Error(err.Error(),
		zap.String("app", conn.Name),
		zap.Error(err),
	)
}

func (conn *Worker) LogInfo(msg string) {
	conn.Logger.Info(msg,
		zap.String("app", conn.Name),
	)
}

func (conn *Worker) deleteMessage(m *sqs.Message) error {
	deleteInput := sqs.DeleteMessageInput{
		ReceiptHandle: m.ReceiptHandle,
		QueueUrl:      &conn.QueueInUrl}
	_, err := conn.Queue.DeleteMessage(&deleteInput)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Worker) sendMessage(msg []byte) error {
	if conn.QueueOutUrl == "" {
		return nil
	}
	_, err := conn.Queue.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    &conn.QueueOutUrl,
	})
	return err
}

func (conn *Worker) Exec(ctx context.Context, m *sqs.Message) ([]byte, error) {
	complete := make(chan *consumerDone)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()

	go func() {
		result, err := conn.Handler(ctx, m)
		complete <- &consumerDone{result, err}
	}()

	select {
	case result := <-complete:
		return result.Result, result.Err
	case <-ctx.Done():
		return nil, errors.New(fmt.Sprint("Message timed out!"))
	}
}

func (conn *Worker) consumer(ctx context.Context, in chan *sqs.Message) {
	for {

		select {
		case <-ctx.Done():
			return
		case msg, running := <-in:
			if !running {
				return
			}

			result, err := conn.Exec(ctx, msg)
			if err != nil {
				conn.LogError(err)
				continue
			}

			err = conn.sendMessage(result)
			if err != nil {
				conn.LogError(err)
				continue
			}

			err = conn.deleteMessage(msg)
			if err != nil {
				conn.LogError(err)
				continue
			}
		}
	}
}

func (conn *Worker) producer(ctx context.Context, out chan *sqs.Message) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(conn.QueueInUrl),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			req, resp := conn.Queue.ReceiveMessageRequest(params)
			err := req.Send()
			if err != nil {
				conn.LogError(err)
			} else {
				messages := resp.Messages
				if len(messages) > 0 {
					for _, message := range messages {
						out <- message
					}
				}
			}
		}
	}
}

func (conn *Worker) Close() {
	conn.done <- nil
}

func (conn *Worker) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan *sqs.Message, 10)

	conn.LogInfo(fmt.Sprint("Staring producer"))
	go func() {
		conn.producer(ctx, messages)
		close(conn.done)
	}()

	go func() {
		<-conn.done
		cancel()
	}()

	conn.LogInfo(fmt.Sprint("Staring consumer with ", conn.Consumers, " consumers"))
	// Consume messages
	var wg sync.WaitGroup
	for x := 0; x < conn.Consumers; x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.consumer(ctx, messages)
		}()
	}
	wg.Wait()
}

func NewWorker(wc WorkerConfig) *Worker {
	session := session.New(&aws.Config{Region: aws.String(wc.Region)})
	logger, _ := zap.NewProduction()
	return &Worker{
		wc.QueueIn,
		wc.QueueOut,
		sqs.New(session),
		session,
		wc.Workers,
		logger,
		wc.Handler,
		wc.Name,
		make(chan error),
	}
}
