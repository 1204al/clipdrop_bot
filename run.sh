#!/usr/bin/env bash
set -euo pipefail

uv run stack --port 8011 --callback-port 8090
