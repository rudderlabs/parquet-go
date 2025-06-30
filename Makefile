.PHONY: test test-run test-teardown sec
PACKAGES=`go list ./... | grep -v example`
TESTFILE=.test-success

GO=go

# go tools versions
govulncheck=golang.org/x/vuln/cmd/govulncheck@latest
gotestsum=gotest.tools/gotestsum@v1.11.0
gitleaks=github.com/zricethezav/gitleaks/v8@v8.18.4

test: install-tools test-run test-teardown

install-tools:
	$(GO) install $(gotestsum)

test-run: ## Run all unit tests
ifeq ($(filter 1,$(debug) $(RUNNER_DEBUG)),)
	$(eval TEST_CMD = SLOW=0 gotestsum --format pkgname-and-test-fails --)
	$(eval TEST_OPTIONS = -race -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=15m)
else
	$(eval TEST_CMD = SLOW=0 go test)
	$(eval TEST_OPTIONS = -race -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=15m)
endif
ifdef package
ifdef exclude
	$(eval FILES = `go list ./$(package)/... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE) || true
else
	$(TEST_CMD) $(TEST_OPTIONS) ./$(package)/... && touch $(TESTFILE) || true
endif
else ifdef exclude
	$(eval FILES = `go list ./... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE) || true
else
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(PACKAGES) && touch $(TESTFILE) || true
endif

test-teardown:
	@if [ -f "$(TESTFILE)" ]; then \
    	echo "Tests passed, tearing down..." ;\
		rm -f $(TESTFILE) ;\
		echo "mode: atomic" > coverage.txt ;\
		find . -name "profile.out" | while read file; do grep -v 'mode: atomic' $${file} >> coverage.txt; rm -f $${file}; done ;\
	else \
    	rm -f coverage.txt coverage.html ; find . -name "profile.out" | xargs rm -f ;\
		echo "Tests failed :-(" ;\
		exit 1 ;\
	fi

coverage:
	$(GO) tool cover -html=coverage.txt -o coverage.html

test-with-coverage: test coverage

.PHONY: sec
sec: ## Run security checks
	$(GO) run $(gitleaks) detect .
	$(GO) run $(govulncheck) ${PACKAGES}
