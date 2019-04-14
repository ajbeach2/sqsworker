test:
	go test -v -coverprofile=cover.out -timeout 30s
cover:
	go tool cover -html=cover.out
vet:
	go vet github.com/ajbeach2/sqsworker
bench:
	go test -bench .
doc:
	godoc -http=localhost:8081
