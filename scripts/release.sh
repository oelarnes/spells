#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <version>  (e.g. $0 0.11.23)"
    exit 1
fi

VERSION="$1"
TAG="v${VERSION}"

if git tag | grep -qx "$TAG"; then
    echo "Tag $TAG already exists"
    exit 1
fi

git tag "$TAG"
git push origin "$TAG"
gh release create "$TAG" --generate-notes --title "$TAG"
