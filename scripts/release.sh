#!/usr/bin/env bash
set -euo pipefail

BUMP="${1:-patch}"
if [[ "$BUMP" != "major" && "$BUMP" != "minor" && "$BUMP" != "patch" ]]; then
    echo "Usage: $0 [major|minor|patch]  (default: patch)"
    exit 1
fi

LATEST=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
if [[ -z "$LATEST" ]]; then
    echo "No existing version tags found"
    exit 1
fi

MAJOR=$(echo "$LATEST" | cut -d. -f1 | tr -d v)
MINOR=$(echo "$LATEST" | cut -d. -f2)
PATCH=$(echo "$LATEST" | cut -d. -f3)

case "$BUMP" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
    patch) PATCH=$((PATCH + 1)) ;;
esac

TAG="v${MAJOR}.${MINOR}.${PATCH}"
echo "Releasing $LATEST -> $TAG"

git tag "$TAG"
git push origin "$TAG"
gh release create "$TAG" --generate-notes --title "$TAG"
