// Copyright 2019 Alexander Beach
// license that can be found in the LICENSE file.

// Package sqsworker implements a SQS consumer that can process sqs messages from a
// SQS queue and optionally send the results to a result tpoic.
//
// Overview
//
// The inenteded use of this package is for multiple consumers reading
// from the same queue. Consumers are represeted by concurrent handler functions
// that are managed by the Worker type. This package only does long-polling based sqs receives.
//
// To use his package, first define a handler function. This can also be a closure:
//
//	var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
//		return []byte(strings.ToLower(*m.Body)), nil
// 	}
//
// The function must match the following type definition:
//
//	type Handler func(context.Context, *sqs.Message) ([]byte, error)
//
// A Worker Struct can be initialized with the NewWorker method, and you may optionally
// define an outbound topic, and number of concurrent workers. If the number of workers
// is not set, the number of workers defaults to runtime.NumCPU().
//
//	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
//	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
//		QueueUrl:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
//		TopicArn: "arn:aws:sns:us-east-1:88888888888:out",
//		Workers:  4,
//		Handler:  handlerFunction,
//		Name:     "TestApp",
//	})
//	w.Run()
//
// The worker will send messages to the TopicArn on successful runs.
//
// Concurrency
//
// Handler function will be called concurrently by multiple workers depending on the configuration.
// It is best to ensure that handler functions can be executed concurrently,
// especially if they are closures and share state.
//
package sqsworker
