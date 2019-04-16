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

// DefaultTimeout for each handler function in seconds
const DefaultTimeout = 30

// DefaultWorkers Number of worker goroutines to spawn, each runs the handler function
const DefaultWorkers = 1

// DefaultMaxNumberOfMessages amount of messages received by each SQS request
const DefaultMaxNumberOfMessages = 10

// DefaultVisibilityTimeout SQS visibility Timeout
const DefaultVisibilityTimeout = 60

// DefaultWaitTimeSeconds Long-polling interval for SQS
const DefaultWaitTimeSeconds = 20

// Handler for SQS consumers
type Handler func(context.Context, *sqs.Message) ([]byte, error)

// Callback which is passed result from handler on success
type Callback func([]byte, error)

// Worker encapsulates the SQS consumer
type Worker struct {
	QueueInURL  string
	QueueOutURL string
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

// WorkerConfig settings for Worker to be passed in NewWorker Contstuctor
type WorkerConfig struct {
	QueueIn  string
	QueueOut string
	// If the number of workers is 0, the number of workers defaults to runtime.NumCPU()
	Workers  int
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

// HandlerTimeoutError for handler function time's out
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
	if w.QueueOutURL == "" {
		return nil
	}
	_, err := w.Queue.SendMessage(msg)
	return err
}

func (w *Worker) exec(ctx context.Context, hp *handlerParams, m *sqs.Message) ([]byte, error) {
	hp.Timer.Reset(w.Timeout)

	go func() {
		result, err := w.Handler(ctx, m)
		hp.Result.Result = result
		hp.Result.Err = err
		hp.Done <- hp.Result
	}()

	select {
	case result := <-hp.Done:
		if !hp.Timer.Stop() {
			<-hp.Timer.C
		}
		return result.Result, result.Err
	case <-hp.Timer.C:
		return nil, &HandlerTimeoutError{}
	}
}

func (w *Worker) consumer(ctx context.Context, in chan *sqs.Message) {
	sendInput := &sqs.SendMessageInput{QueueUrl: &w.QueueOutURL}
	deleteInput := &sqs.DeleteMessageInput{QueueUrl: &w.QueueInURL}
	hanlderInput := w.getHandlerParams()
	var msgString string

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-in:
			result, err := w.exec(ctx, hanlderInput, msg)
			if err == nil {
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
			} else {
				w.logError("handler failed!", err)
			}

			if w.Callback != nil {
				w.Callback(result, err)
			}
		}
	}
}

func (w *Worker) producer(ctx context.Context, out chan *sqs.Message) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(w.QueueInURL),
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
				w.logError("receive messages failed!", err)
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

// Close function will send a signal to all workers to exit
func (w *Worker) Close() {
	close(w.done)
}

// Run does the main consumer/producer loop
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

// NewWorker constructor for SQS Worker
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
