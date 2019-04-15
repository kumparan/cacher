ifdef test_run
	TEST_ARGS := -run $(test_run)
endif

GOCOMMAND := go
ifneq (, $(shell which richgo))
	GOCOMMAND = richgo
endif

test_command=$(GOCOMMAND) test ./... $(TEST_ARGS) -v --cover

test:
	$(test_command)
