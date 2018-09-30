cover:
	go tool cover -html=cover.out
test:
	go test -v -coverprofile=cover.out
vet:
	go vet github.com/ajbeach2/worker