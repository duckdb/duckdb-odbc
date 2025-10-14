#!/bin/bash

# Check for commit message
if [ -z "$1" ]; then
  echo "Usage: $0 \"Your commit message\""
  exit 1
fi

# Run formatter
make format-fix

# Stage all changes except test/tmp.test
git add -u  # stage modified/deleted files
git add .   # stage new files
git reset test/tmp.test local/* 2>/dev/null  # unstage if present

# Check if there are staged changes
if git diff --cached --quiet; then
  echo "No changes to commit."
  exit 0
fi


git commit -m "$1"

# Only push if -p is passed
if [[ "$1" == "-p" ]]; then
  git push
  echo "Skipping push due to -p flag."
  exit 0
fi

