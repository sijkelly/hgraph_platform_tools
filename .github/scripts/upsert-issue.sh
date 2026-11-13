#!/usr/bin/env bash
# Open an issue, or comment on the existing open one carrying the same label.
#
# Usage: upsert-issue.sh <label> <title> <body-file>
#
# One open issue per label keeps a weekly job from filing a fresh issue every
# week while the same breakage persists; each run adds a comment instead.
# Requires GH_TOKEN with issues: write, and GH_REPO (or run inside a checkout).
set -euo pipefail

label="$1"
title="$2"
body_file="$3"

gh label create "$label" --color B60205 --description "Raised by scheduled upstream checks" --force >/dev/null

existing="$(gh issue list --state open --label "$label" --limit 1 --json number --jq '.[0].number // empty')"
if [[ -n "$existing" ]]; then
  gh issue edit "$existing" --title "$title" >/dev/null
  gh issue comment "$existing" --body-file "$body_file"
  echo "Updated issue #$existing"
else
  gh issue create --title "$title" --label "$label" --body-file "$body_file"
fi
