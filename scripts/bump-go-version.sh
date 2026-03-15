#!/usr/bin/env bash
#
# Bump the Go version across all go.mod files and CI workflow.
#
# Usage: ./scripts/bump-go-version.sh <new-version>
# Example: ./scripts/bump-go-version.sh 1.26.1
#
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 1.26.1"
    exit 1
fi

NEW_VERSION="$1"

# Validate format: major.minor or major.minor.patch
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    echo "Error: version must be in format X.Y or X.Y.Z (got: $NEW_VERSION)"
    exit 1
fi

# Extract major.minor for CI workflow (e.g., 1.26.1 -> 1.26)
if [[ "$NEW_VERSION" =~ ^([0-9]+\.[0-9]+)\.[0-9]+$ ]]; then
    MINOR_VERSION="${BASH_REMATCH[1]}"
else
    # Already major.minor (e.g., "1.26"), use as-is
    MINOR_VERSION="$NEW_VERSION"
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Bumping Go version to $NEW_VERSION (CI: $MINOR_VERSION)"
echo ""

# Update go.mod files
GO_MOD_FILES=(
    "$REPO_ROOT/go.mod"
    "$REPO_ROOT/prestoauth/kerberos/go.mod"
    "$REPO_ROOT/prestoauth/oauth2/go.mod"
)

for mod_file in "${GO_MOD_FILES[@]}"; do
    if [ -f "$mod_file" ]; then
        old=$(grep '^go ' "$mod_file" | awk '{print $2}')
        sed -i '' "s/^go .*/go $NEW_VERSION/" "$mod_file"
        echo "  $mod_file: $old -> $NEW_VERSION"
    else
        echo "  WARNING: $mod_file not found, skipping"
    fi
done

# Update CI workflow
CI_FILE="$REPO_ROOT/.github/workflows/ci.yml"
if [ -f "$CI_FILE" ]; then
    old=$(grep 'go-version:' "$CI_FILE" | head -1 | sed 's/.*go-version: *"\([^"]*\)".*/\1/')
    sed -i '' "s/go-version: \"[0-9.]*\"/go-version: \"$MINOR_VERSION\"/g" "$CI_FILE"
    echo "  $CI_FILE: $old -> $MINOR_VERSION"
else
    echo "  WARNING: $CI_FILE not found, skipping"
fi

echo ""
echo "Done. Next steps:"
echo "  1. Run 'go mod tidy' in root and each submodule"
echo "  2. Run 'go test ./... -race -count=1'"
echo "  3. Run 'go vet ./...' and 'staticcheck ./...'"
echo "  4. Run 'govulncheck ./...' (rebuild with 'go install golang.org/x/vuln/cmd/govulncheck@latest' if needed)"
