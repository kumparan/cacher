ifdef test_run
	TEST_ARGS := -run $(test_run)
endif

GOCOMMAND := go
ifneq (, $(shell which richgo))
	GOCOMMAND = richgo
endif

test_command=$(GOCOMMAND) test ./... $(TEST_ARGS) -v --cover

check-cognitive-complexity:
	-gocognit -over 15 .

lint: check-cognitive-complexity
	golangci-lint run

test: lint test-only

test-only:
	$(test_command)