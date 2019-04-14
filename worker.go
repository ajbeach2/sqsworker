package sqsworker

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"runtime"
	"sync"
	"time"
)

// Timeout for each handler function in seconds
const DefaultTimeout = 30

// Number of worker goroutines to spawn, each runs the handler function
const DefaultWorkers = 1

// Default amount of messages recieved by each SQS request
const DefaultMaxNumberOfMessages = 10

// SQS visibility Timeout
const DefaultVisibilityTimeout = 60

// Long-polling interval for SQS
const DefaultWaitTimeSeconds = 20

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
	// If the number of workers is 0, the number of workers defaults to runtime.NumCPU()
	Workers  int
	Region   string
	Handler  Handler
	Callback Callback
	Name     string
	Timeout  int
	Logger   *zap.Logger
}

type consumerDone struct {
	Result []byte
	Err    error
}

type HandlerTimeoutError struct{}

func (HandlerTimeoutError) Error() string {
	return "Handler Timeout!"
}

type handlerParams struct {
	Done   chan *consumerDone
	Result *consumerDone
	Timer  *time.Timer
}

func (w *Worker) getHandlerParams() *handlerParams {
	return &handlerParams{
		make(chan *consumerDone),
		&consumerDone{},
		time.NewTimer(w.Timeout),
	}
}

func (w *Worker) logError(msg string, err error) {
	if w.Logger != nil {
		w.Logger.Error(err.Error(),
			zap.String("app", w.Name),
			zap.String("msg", msg),
			zap.Error(err),
		)
	}
}

func (w *Worker) logInfo(msg string) {
	if w.Logger != nil {
		w.Logger.Info(msg,
			zap.String("app", w.Name),
		)
	}
}

func (w *Worker) deleteMessage(m *sqs.DeleteMessageInput) error {
	_, err := w.Queue.DeleteMessage(m)
	if err != nil {
		return err
	}
	return nil
}

func (w *Worker) sendMessage(msg *sqs.SendMessageInput) error {
	if w.QueueOutUrl == "" {
		return nil
	}
	_, err := w.Queue.SendMessage(msg)
	return err
}

func (w *Worker) exec(ctx context.Context, hp *handlerParams, m *sqs.Message) ([]byte, error) {
	if !hp.Timer.Stop() {
		<-hp.Timer.C
	}
	hp.Timer.Reset(w.Timeout)

	go func() {
		result, err := w.Handler(ctx, m)
		hp.Result.Result = result
		hp.Result.Err = err
		hp.Done <- hp.Result
	}()

	select {
	case result := <-hp.Done:
		return result.Result, result.Err
	case <-hp.Timer.C:
		return nil, &HandlerTimeoutError{}
	}
}

func (w *Worker) consumer(ctx context.Context, in chan *sqs.Message) {
	sendInput := &sqs.SendMessageInput{QueueUrl: &w.QueueOutUrl}
	deleteInput := &sqs.DeleteMessageInput{QueueUrl: &w.QueueInUrl}
	hanlderInput := w.getHandlerParams()
	var msgString string

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-in:
			result, err := w.exec(ctx, hanlderInput, msg)
			if w.Callback != nil {
				w.Callback(result, err)
			}
			if err != nil {
				w.logError("handler failed!", err)
				continue
			}
			msgString = string(result)
			sendInput.MessageBody = &msgString
			err = w.sendMessage(sendInput)
			if err != nil {
				w.logError("send message failed!", err)
				continue
			}

			deleteInput.ReceiptHandle = msg.ReceiptHandle
			err = w.deleteMessage(deleteInput)
			if err != nil {
				w.logError("delete message failed!", err)
				continue
			}
		}
	}
}

func (w *Worker) producer(ctx context.Context, out chan *sqs.Message) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(w.QueueInUrl),
		MaxNumberOfMessages: aws.Int64(DefaultMaxNumberOfMessages),
		VisibilityTimeout:   aws.Int64(DefaultVisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(DefaultWaitTimeSeconds),
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			req, resp := w.Queue.ReceiveMessageRequest(params)
			err := req.Send()
			if err != nil {
				w.logError("recieve messages failed!", err)
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
	close(w.done)
}

func (w *Worker) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan *sqs.Message, w.Consumers)

	w.logInfo(fmt.Sprint("Staring producer"))
	go func() {
		w.producer(ctx, messages)
		close(messages)
	}()

	go func() {
		<-w.done
		cancel()
	}()

	w.logInfo(fmt.Sprint("Staring consumer with ", w.Consumers, " consumers"))
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
}

func NewWorker(sess *session.Session, wc WorkerConfig) *Worker {
	var logger *zap.Logger
	var timeout = wc.Timeout
	workers := runtime.NumCPU()

	if wc.Timeout == 0 {
		timeout = DefaultTimeout
	}

	if wc.Workers != 0 {
		workers = wc.Workers
	}

	if wc.Logger == nil {
		logger, _ = zap.NewProduction()
	} else {
		logger = wc.Logger
	}

	return &Worker{
		wc.QueueIn,
		wc.QueueOut,
		sqs.New(sess),
		sess,
		workers,
		logger,
		wc.Handler,
		wc.Callback,
		wc.Name,
		time.Duration(timeout) * time.Second,
		make(chan error),
	}
}
