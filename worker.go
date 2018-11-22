package worker

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"sync"
)

type Callback func(m *sqs.Message) error

type Connection struct {
	QueueUrl  string
	Queue     sqsiface.SQSAPI
	Session   *session.Session
	Consumers int
	Logger    *zap.Logger
	App       string
}

type WorkerConfig struct {
	QueueUrl string
	Workers  int
	Region   string
	App      string
}

func (conn *Connection) LogError(err error) {
	defer conn.Logger.Sync()
	conn.Logger.Error(err.Error(),
		zap.String("App", conn.App),
		zap.Error(err),
	)
}

func (conn *Connection) LogInfo(msg string) {
	defer conn.Logger.Sync()
	conn.Logger.Info(msg,
		zap.String("App", conn.App),
	)
}

func (conn *Connection) deleteMessage(m *sqs.Message) error {
	deleteInput := sqs.DeleteMessageInput{
		ReceiptHandle: m.ReceiptHandle,
		QueueUrl:      &conn.QueueUrl}
	_, err := conn.Queue.DeleteMessage(&deleteInput)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) consume(cb Callback, in chan *sqs.Message, errors chan error) {
	for {
		select {
		case msg, running := <-in:
			if !running {
				return
			}
			err := cb(msg)
			if err != nil {
				errors <- err
			} else {
				err = conn.deleteMessage(msg)
				if err != nil {
					errors <- err
				}
			}
		}
	}
}

func (conn *Connection) produce(ctx context.Context, params *sqs.ReceiveMessageInput, out chan *sqs.Message, errors chan error) {
	for {
		select {
		case <-ctx.Done():
			close(out)
			return
		default:
			req, resp := conn.Queue.ReceiveMessageRequest(params)
			err := req.Send()
			if err != nil {
				errors <- err
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

func (conn *Connection) Run(ctx context.Context, cb Callback) {
	messages := make(chan *sqs.Message, 10)
	errors := make(chan error)

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(conn.QueueUrl),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	}

	go func() {
		conn.produce(ctx, params, messages, errors)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errors:
				conn.LogError(err)
			}
		}
	}()

	conn.LogInfo(fmt.Sprint("Staring consumer with ", conn.Consumers, " consumers"))
	// Consume messages
	var wg sync.WaitGroup
	for x := 0; x < conn.Consumers; x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.consume(cb, messages, errors)
		}()
	}
	wg.Wait()
}

func NewConnection(wc WorkerConfig) *Connection {
	session := session.New(&aws.Config{Region: aws.String(wc.Region)})
	logger, _ := zap.NewProduction()
	return &Connection{
		wc.QueueUrl,
		sqs.New(session),
		session,
		wc.Workers,
		logger,
		wc.App,
	}
}
