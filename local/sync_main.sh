#!/bin/bash

# Exit on any error
set -e

# go to the root of the repository
cd "$(git rev-parse --show-toplevel)" || exit 1

# ensure local main is checked out
git checkout main

# make sure remotes are up to date
git fetch origin
git fetch upstream

# merge upstream/main into local main
git merge upstream/main

# push merged changes to origin
git push origin main

echo "Successfully merged upstream changes into main and pushed to origin."
