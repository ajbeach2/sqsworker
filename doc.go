// Copyright 2020 Alexander Beach
// license that can be found in the LICENSE file.

// Package sqsworker implements a SQS consumer that can process sqs messages from a
// SQS queue and optionally send the results to a result topic.
//
// Overview
//
// The inenteded use of this package is for multiple consumers reading
// from the same queue. Consumers are represented by structs that implement the Processor interface,
// which are managed by the Worker type. This package *only* does long-polling based sqs receives.
//
// To use this package, you must implement the following interface:
//
//	type Processor interface {
//		Process(context.Context, *sqs.Message, *sns.PublishInput) error
//	}
//
// For example:
//
//	import (
//	"context"
//	"github.com/ajbeach2/sqsworker"
//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/session"
//	"github.com/aws/aws-sdk-go/service/sns"
//	"github.com/aws/aws-sdk-go/service/sqs"
//	"strings"
// )

//	type LowerCaseWorker struct {
//	}

//	func (l *LowerCaseWorker) Process(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
//		*w.Message = strings.ToLower(*m.Body)
//		return nil
//	}

//	func ExampleWorker() {
//		lowerCaseWorker := &LowerCaseWorker{}
//		sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

//		queueURL, _ := sqsworker.GetOrCreateQueue("In", sqs.New(sess))
//		topicArn, _ := sqsworker.GetOrCreateTopic("Out", sns.New(sess))
//
//		w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
//			QueueURL:  queueURL,
//			TopicArn:  topicArn,
//			Workers:   1,
//			Processor: lowerCaseWorker,
//			Name:      "TestApp",
//		})
//		w.Run()
//	}
//
//
// A Worker Struct can be initialized with the NewWorker method, and you may optionally
// define an outbound topic, and number of concurrent workers. If the number of workers
// is not set, the number of workers defaults to runtime.NumCPU().  There are helper functions
// provided for getting or creating topcis and queues.
// The worker will send messages to the TopicArn on successful runs.
//
// Concurrency
//
// The Process function defined by the Processor interface will be called concurrently by multiple workers depending on the configuration.
// It is best to ensure that Process functions can be executed concurrently.
//
package sqsworker
