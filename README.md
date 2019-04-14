# sqsworker
sqs consumer written in go

## Overview

The Worker type represents a SQS consumer that can process sqs messages from a
SQS queue and optionally send the results to a queue. The inenteded use is
multiple concurrent consumers reading from the same queue which execute the
hander functions defined on the Worker struct.

To use his package, first define a handler function. This can also be a closure:

```go
var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
	return []byte(strings.ToLower(*m.Body)), nil
}
 ```

The function must match the following type definition:

```go
type Handler func(context.Context, *sqs.Message) ([]byte, error)
```

A Worker Struct can be initialized with the NewWorker method, and you may optionally
define an outbound queue, and number of concurrent workers. If the number of workers
is not set, the number of workers defaults to runtime.NumCPU(). A Timeout in seconds
can be set where a handler will fail on a Timeout. The default, if this is not set, is 30 seconds.

```go
w := worker.NewWorker(sess, worker.WorkerConfig{
	QueueIn:  "https:sqs.us-east-1.amazonaws.com/88888888888/In",
	QueueOut: "https:sqs.us-east-1.amazonaws.com/88888888888/Out",
	Workers:  4,
	Timeout,  30, // Handler Timeout in seconds.
	Handler:  handlerFunction,
	Name:     "TestApp",
})
w.Run()
```  

The worker will send messages to the QueueOut queue on succesfull runs.

## Concurrency

Handler function will be called concurrently by multiple workers depending on the configuration,
and it is best to ensure that handler functions can be executed concurrently, especially if they are closures and share state.

## Performance

Real world performace will be dictated by latency to sqs. The benchmarks mock sqs calls to illustrate that
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
BenchmarkHello-4   	 1000000	      1395 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/ajbeach2/sqsworker	2.427s

```



