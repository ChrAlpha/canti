.PHONY: check fmt integration test vet

check: fmt vet test

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...

integration:
	go test -count=1 -race -tags integration ./...
