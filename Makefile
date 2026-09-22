.PHONY: check fmt test vet

check: fmt vet test

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...
