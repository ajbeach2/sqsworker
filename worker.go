package worker

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"log"
	"sync"
)

type Callback func(m *sqs.Message) error

type Connection struct {
	QueueUrl string
	Queue    sqsiface.SQSAPI
	Session  *session.Session
}

func (conn *Connection) deleteMessage(m *sqs.Message) {
	deleteInput := sqs.DeleteMessageInput{
		ReceiptHandle: m.ReceiptHandle,
		QueueUrl:      &conn.QueueUrl}
	_, err := conn.Queue.DeleteMessage(&deleteInput)
	if err != nil {
		log.Println(err)
	}
}

func (conn *Connection) consume(cb Callback, in chan *sqs.Message) {
	for {
		select {
		case msg, running := <-in:
			if !running {
				return
			}
			err := cb(msg)
			if err != nil {
				log.Println(err)
			} else {
				conn.deleteMessage(msg)
			}
		}
	}
}

func (conn *Connection) produce(ctx context.Context, params *sqs.ReceiveMessageInput, out chan *sqs.Message) {
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down consumer...")
			close(out)
			return
		default:
			req, resp := conn.Queue.ReceiveMessageRequest(params)
			err := req.Send()
			if err != nil {
				log.Println(err)
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

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(conn.QueueUrl),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	}

	go func() {
		conn.produce(ctx, params, messages)
	}()

	// Consume messages
	var wg sync.WaitGroup
	for x := 0; x < 4; x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.consume(cb, messages)
		}()
	}
	wg.Wait()
}

func NewConnection(queueUrl string) *Connection {
	session := session.New(&aws.Config{Region: aws.String("us-east-1")})
	return &Connection{
		queueUrl,
		sqs.New(session),
		session,
	}
}
