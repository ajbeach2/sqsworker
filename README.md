# worker
sqs consumer written in go


## Example
```go
var handlerFunction = func(ctx context.Context, m *sqs.Message) ([]byte, error) {
    return []byte(strings.ToLower(*m.Body)), nil
}

w := worker.NewWorker(worker.WorkerConfig{
    QueueIn:  "https://sqs.us-east-1.amazonaws.com/88888888888/In",
    QueueOut: "https://sqs.us-east-1.amazonaws.com/88888888888/Out",
    Workers:  1,
    Region:   "us-east-1",
    Handler:  handlerFunction,
    Name:     "TestApp",
})

w.Run()
```