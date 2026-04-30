#!/usr/bin/env bash
set -euo pipefail

BUMP="patch"
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        major|minor|patch) BUMP="$arg" ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Usage: $0 [major|minor|patch] [--dry-run]"; exit 1 ;;
    esac
done

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
BRANCH=$(git rev-parse --abbrev-ref HEAD)

if $DRY_RUN; then
    echo "Dry run: $LATEST -> $TAG"
    echo "Branch: $BRANCH$([ "$BRANCH" != "main" ] && echo " (WARNING: not on main)")"
    echo ""
    echo "Latest commit:"
    git log -1 --format="  %h %s (%an, %ar)"
    echo ""
    echo "Commits since $LATEST:"
    git log "${LATEST}..HEAD" --format="  %h %s"
    exit 0
fi

if [[ "$BRANCH" != "main" ]]; then
    echo "Error: must be on main to release (currently on $BRANCH)"
    exit 1
fi

echo "Releasing $LATEST -> $TAG"
git tag "$TAG"
git push origin "$TAG"
gh release create "$TAG" --generate-notes --title "$TAG"
