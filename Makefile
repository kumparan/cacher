ifdef test_run
	TEST_ARGS := -run $(test_run)
endif

GOCOMMAND := go
ifneq (, $(shell which richgo))
	GOCOMMAND = richgo
endif

test_command=$(GOCOMMAND) test ./... $(TEST_ARGS) -v --cover

check-cognitive-complexity:
	-gocognit -over 28 .

lint: check-cognitive-complexity
	golangci-lint run --print-issued-lines=false --exclude-use-default=false --enable=revive --enable=goimports  --enable=unconvert --concurrency=2

test: lint
	$(test_command)
