#!/bin/bash

# Generate a list of markdown formatted changes since the supplied `previous-commit`. 
# You'd run this script like this:
# ./scripts/get_changes_since_commit.sh --previous-commit 1d2e083acdd88d0829ad31e3a1725e3898565fda

while [ "$#" -gt 0 ]; do
  case "$1" in
    --previous-commit)
      previouscommit=($2)
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [ -z "$previouscommit" ]; then
  echo "Missing --previous-commit"
  exit 1
fi

currentcommit=$(git rev-parse HEAD)

# Get the commit messages between the previous commit and now.
messages=$(git log --pretty=format:"%h %s" $previouscommit..$currentcommit)

function geturl() {
  echo "https://github.com/aptos-labs/aptos-indexer-processors/commit/$1"
}

cat <<EOF
Full changes since $previouscommit (newest first):
EOF

while IFS= read -r line; do
  sha=$(echo "$line" | cut -d ' ' -f1)
  message=$(echo "$line" | cut -d ' ' -f2-)
  echo "- [$sha]($(geturl $sha)): $message"
done <<< "$messages"

cat <<EOF
EOF
