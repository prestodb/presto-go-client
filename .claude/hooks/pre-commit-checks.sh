#!/bin/bash
# Pre-commit hook: runs fmt, vet, tests, and coverage across all modules

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# Only intercept git commit commands
if ! echo "$COMMAND" | grep -qE 'git\s+commit'; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 2

FAILED=0

# Format check (root + submodules)
echo "Checking formatting..."
UNFORMATTED=$(gofmt -l . && gofmt -l prestoauth/kerberos/ && gofmt -l prestoauth/oauth2/)
if [ -n "$UNFORMATTED" ]; then
  echo "Files not formatted:" >&2
  echo "$UNFORMATTED" >&2
  FAILED=1
fi

# Vet (root + submodules)
echo "Running go vet..."
if ! go vet ./... 2>&1; then
  FAILED=1
fi
if ! (cd prestoauth/kerberos && go vet ./...) 2>&1; then
  FAILED=1
fi
if ! (cd prestoauth/oauth2 && go vet ./...) 2>&1; then
  FAILED=1
fi

# Tests with race detection (root + submodules)
echo "Running tests..."
if ! go test ./... -race -count=1 2>&1; then
  FAILED=1
fi
if ! (cd prestoauth/kerberos && go test ./... -race -count=1) 2>&1; then
  FAILED=1
fi
if ! (cd prestoauth/oauth2 && go test ./... -race -count=1) 2>&1; then
  FAILED=1
fi

# Coverage threshold (80%)
echo "Checking coverage..."
go test ./... -coverprofile=coverage.out -covermode=atomic \
  -coverpkg=github.com/prestodb/presto-go-client,github.com/prestodb/presto-go-client/utils 2>&1
COVERAGE=$(go tool cover -func=coverage.out 2>/dev/null | grep total | awk '{print $3}' | sed 's/%//')
if [ -n "$COVERAGE" ]; then
  if (( $(echo "$COVERAGE < 80" | bc -l) )); then
    echo "Coverage ${COVERAGE}% is below 80% threshold" >&2
    FAILED=1
  else
    echo "Coverage: ${COVERAGE}%"
  fi
fi
rm -f coverage.out

if [ "$FAILED" -eq 1 ]; then
  echo "Pre-commit checks failed. Fix issues before committing." >&2
  exit 2
fi

echo "All checks passed."
exit 0
