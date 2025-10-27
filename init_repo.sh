#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: ./init_repo.sh <github_repo_https_url>"
  echo "Example: ./init_repo.sh https://github.com/<user>/opspulse-analytics.git"
  exit 1
fi

REPO_URL="$1"

git init
git add .
git commit -m "Initial commit: OpsPulse analytics"
git branch -M main
git remote add origin "$REPO_URL"
git push -u origin main
