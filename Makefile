.PHONY: test lint deps dev

test:
	go test ./...

lint:
	golangci-lint run ./...

deps:
	go mod tidy

# Enable local development with go.work
dev:
	@if [ ! -f go.work ]; then \
		echo "go 1.25.6\n\nuse .\n\nreplace (\n\tgithub.com/clerk/jack-courier-lib v0.0.0 => ../jack-courier-lib\n\tgithub.com/clerk/jack-service/proto/jackpb v0.2.2 => ../jack-service/proto/jackpb\n)" > go.work; \
		echo "go.work created for local development"; \
	else \
		echo "go.work already exists"; \
	fi
