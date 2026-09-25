#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEY_FILE="$PROJECT_ROOT/airflow-jwt-key.txt"

if ! command -v openssl >/dev/null 2>&1; then
    echo "ERROR: openssl is required to generate the Airflow JWT secret." >&2
    exit 1
fi

SECRET_VALUE="$(openssl rand -hex 32)"
umask 077
TEMP_FILE="$(mktemp "$PROJECT_ROOT/.airflow-jwt-key.txt.tmp.XXXXXX")"
trap 'rm -f "$TEMP_FILE"' EXIT

printf '%s\n' "$SECRET_VALUE" > "$TEMP_FILE"
chmod 600 "$TEMP_FILE"
mv "$TEMP_FILE" "$KEY_FILE"
trap - EXIT
unset SECRET_VALUE

echo "Updated Airflow JWT secret file: $KEY_FILE"
echo "Restart all Airflow components so they use the same secret."
