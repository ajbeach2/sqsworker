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
	"time"
)

const DefaultTimeout = 30
const DefaultWorkers = 1
const MaxNumberOfMessages = 10
const VisibilityTimeout = 10
const WaitTimeSeconds = 20

type Handler func(context.Context, *sqs.Message) ([]byte, error)
type Callback func([]byte, error)

type Worker struct {
	QueueInUrl  string
	QueueOutUrl string
	Queue       sqsiface.SQSAPI
	Session     *session.Session
	Consumers   int
	Logger      *zap.Logger
	Handler     Handler
	Callback    Callback
	Name        string
	Timeout     time.Duration
	done        chan error
}

type WorkerConfig struct {
	QueueIn  string
	QueueOut string
	Workers  int
	Region   string
	Handler  Handler
	Callback Callback
	Name     string
	Timeout  int
}

type consumerDone struct {
	Result []byte
	Err    error
}

type HandlerTimeout struct{}

func (HandlerTimeout) Error() string {
	return "Handler Timeout!"
}

func (w *Worker) LogError(err error) {
	w.Logger.Error(err.Error(),
		zap.String("app", w.Name),
		zap.Error(err),
	)
}

func (w *Worker) LogInfo(msg string) {
	w.Logger.Info(msg,
		zap.String("app", w.Name),
	)
}

func (w *Worker) deleteMessage(m *sqs.Message) error {
	deleteInput := sqs.DeleteMessageInput{
		ReceiptHandle: m.ReceiptHandle,
		QueueUrl:      &w.QueueInUrl}
	_, err := w.Queue.DeleteMessage(&deleteInput)
	if err != nil {
		return err
	}
	return nil
}

func (w *Worker) sendMessage(msg []byte) error {
	if w.QueueOutUrl == "" {
		return nil
	}
	_, err := w.Queue.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    &w.QueueOutUrl,
	})
	return err
}

func (w *Worker) Exec(ctx context.Context, m *sqs.Message) ([]byte, error) {
	complete := make(chan *consumerDone)

	go func() {
		result, err := w.Handler(ctx, m)
		complete <- &consumerDone{result, err}
	}()
	select {
	case result := <-complete:
		return result.Result, result.Err
	case <-time.After(w.Timeout):
		return nil, &HandlerTimeout{}
	}
}

func (w *Worker) consumer(ctx context.Context, in chan *sqs.Message) {
	for {

		select {
		case <-ctx.Done():
			return
		case msg, running := <-in:
			if !running {
				return
			}

			result, err := w.Exec(ctx, msg)
			if w.Callback != nil {
				w.Callback(result, err)
			}
			if err != nil {
				w.LogError(err)
				continue
			}

			err = w.sendMessage(result)
			if err != nil {
				w.LogError(err)
				continue
			}

			err = w.deleteMessage(msg)
			if err != nil {
				w.LogError(err)
				continue
			}
		}
	}
}

func (w *Worker) producer(ctx context.Context, out chan *sqs.Message) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(w.QueueInUrl),
		MaxNumberOfMessages: aws.Int64(MaxNumberOfMessages),
		VisibilityTimeout:   aws.Int64(VisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(WaitTimeSeconds),
	}
	for {
		select {
		case <-ctx.Done():
			close(out)
			return
		default:
			req, resp := w.Queue.ReceiveMessageRequest(params)
			err := req.Send()
			if err != nil {
				w.LogError(err)
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

func (w *Worker) Close() {
	w.done <- nil
}

func (w *Worker) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan *sqs.Message, 10)

	w.LogInfo(fmt.Sprint("Staring producer"))
	go func() {
		w.producer(ctx, messages)
	}()

	go func() {
		<-w.done
		cancel()
	}()

	w.LogInfo(fmt.Sprint("Staring consumer with ", w.Consumers, " consumers"))
	// Consume messages
	var wg sync.WaitGroup
	for x := 0; x < w.Consumers; x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.consumer(ctx, messages)
		}()
	}
	wg.Wait()
	close(w.done)
}

func NewWorker(wc WorkerConfig) *Worker {
	session := session.New(&aws.Config{Region: aws.String(wc.Region)})
	logger, _ := zap.NewProduction()
	var timeout = wc.Timeout

	if wc.Timeout == 0 {
		timeout = DefaultTimeout
	}

	return &Worker{
		wc.QueueIn,
		wc.QueueOut,
		sqs.New(session),
		session,
		wc.Workers,
		logger,
		wc.Handler,
		wc.Callback,
		wc.Name,
		time.Duration(timeout) * time.Second,
		make(chan error),
	}
}
