// Copyright 2019 Alexander Beach
// license that can be found in the LICENSE file.

// Package worker implements a SQS Consumer
//
// Overview
//
// The Worker type represents a SQS consumer that can process sqs messages from a
// SQS queue and optionally send the results to a result queue. The inenteded use
// multiple consumers reading from the same queue. Consumers are represeted by
// concurrently handler functions that are managed by the Worker type. This package
// only does long-polling based sqs recieves.
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
// define an outbound queue, and number of concurrent workers. If the number of workers
// is set to 0, the number of workers defaults to runtime.NumCPU()
//
//	w := worker.NewWorker(sess, worker.WorkerConfig{
// 		QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
//		QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
//		Workers:  4,
//		Handler:  handlerFunction,
//		Name:     "TestApp",
//	})
//	w.Run()
//
// The worker will send messages to the QueuOut queue on succesfull runs.
//
// Concurrency
//
// Handler function will be called concurrently by multiple workers depending on the configuration,
// and it is best to ensure that handler functions can safely be executed concurrently.
//
package worker
