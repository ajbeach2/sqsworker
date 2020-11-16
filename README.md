# sqsworker 
Package sqsworker implements a SQS consumer that can process sqs messages from a SQS queue and optionally send the results to a result topic.

⚠️WARNING⚠️: This repo is under active development, and there may be rapid and incompatible changes.
 

[![CircleCI](https://circleci.com/gh/ajbeach2/sqsworker/tree/master.svg?style=svg)](https://circleci.com/gh/ajbeach2/sqsworker/tree/master)
[![GoDoc](https://godoc.org/github.com/ajbeach2/sqsworker?status.svg)](https://godoc.org/github.com/ajbeach2/sqsworker)
[![Maintainability](https://api.codeclimate.com/v1/badges/a1b4d81620ea0c71f47c/maintainability)](https://codeclimate.com/github/ajbeach2/sqsworker/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a1b4d81620ea0c71f47c/test_coverage)](https://codeclimate.com/github/ajbeach2/sqsworker/test_coverage)
[![Go Report Card](https://goreportcard.com/badge/github.com/ajbeach2/sqsworker)](https://goreportcard.com/report/github.com/ajbeach2/sqsworker)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/ajbeach2/sqsworker/blob/master/LICENSE)
[![Release](https://img.shields.io/github/release/ajbeach2/sqsworker.svg)](https://github.com/ajbeach2/sqsworker/releases)

## Overview

The inenteded use of this package is for multiple consumers reading from the same queue. Consumers are represented by structs that implement the Processor interface, which are managed by the Worker type. This package *only* does long-polling based sqs receives.

To use this package, you must implement the following interface:
```go
type Processor interface {
	Process(context.Context, *sqs.Message, *sns.PublishInput) error
}
```
For example:
```go
import (
	"context"
	"github.com/ajbeach2/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

type LowerCaseWorker struct {
}

func (l *LowerCaseWorker) Process(ctx context.Context, m *sqs.Message, w *sns.PublishInput) error {
	*w.Message = strings.ToLower(*m.Body)
	return nil
}

func ExampleWorker() {

	lowerCaseWorker := &LowerCaseWorker{}
	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

	queueURL, _ := sqsworker.GetOrCreateQueue("In", sqs.New(sess))
	topicArn, _ := sqsworker.GetOrCreateTopic("Out", sns.New(sess))

	w := sqsworker.NewWorker(sess, sqsworker.WorkerConfig{
		QueueURL:  queueURL,
		TopicArn:  topicArn,
		Workers:   1,
		Processor: lowerCaseWorker,
		Name:      "TestApp",
	})

	w.Run()
}
```

A Worker Struct can be initialized with the NewWorker method, and you may optionally
define an outbound topic, and number of concurrent workers. If the number of workers
is not set, the number of workers defaults to runtime.NumCPU().  There are helper functions
provided for getting or creating topcis and queues.
The worker will send messages to the TopicArn on successful runs.

## Concurrency

The Process function defined by the Processor interface will be called concurrently by multiple workers depending on the configuration. It is best to ensure that Process functions can be executed concurrently.

## Performance

Real world performace will be dictated by latency to sqs. The benchmarks mock sqs and sns calls to illustrate that
the package adds very little overhead to consuming messages, and to ensure that memory is managed to not
create more garbage collection than needed.

From the SQS documentation in AWS:
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-throughput-horizontal-scaling-and-batching.html

> Because you access Amazon SQS through an HTTP request-response protocol, the request latency (the interval between initiating a request and receiving a response) limits the throughput that you can achieve from a single thread using a single connection. For example, if the latency from an Amazon EC2-based client to Amazon SQS in the same region averages 20 ms, the maximum throughput from a single thread over a single connection averages 50 TPS.

```bash
->cat /proc/cpuinfo | grep "model name" | head -1
model name	: Intel(R) Core(TM) i5-6600K CPU @ 3.50GHz

->go test -bench .
goos: linux
goarch: amd64
pkg: github.com/ajbeach2/sqsworker
BenchmarkWorkerSQSSNS-4   	 1476417	       862 ns/op	      64 B/op	       1 allocs/op
BenchmarkWorkerSQS-4      	 2012392	       561 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/ajbeach2/sqsworker	3.835s

```
