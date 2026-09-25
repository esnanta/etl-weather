#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
ENV_KEY="AIRFLOW__API_AUTH__JWT_SECRET"

if ! command -v openssl >/dev/null 2>&1; then
    echo "ERROR: openssl is required to generate the Airflow JWT secret." >&2
    exit 1
fi

SECRET_VALUE="$(openssl rand -hex 32)"
umask 077
TEMP_FILE="$(mktemp "$PROJECT_ROOT/.env.tmp.XXXXXX")"
trap 'rm -f "$TEMP_FILE"' EXIT

if [[ -f "$ENV_FILE" ]]; then
    awk -v key="$ENV_KEY" -v value="$SECRET_VALUE" '
        BEGIN { updated = 0 }
        $0 ~ "^[[:space:]]*(export[[:space:]]+)?" key "=" {
            if (!updated) {
                print key "=" value
                updated = 1
            }
            next
        }
        { print }
        END {
            if (!updated) {
                print key "=" value
            }
        }
    ' "$ENV_FILE" > "$TEMP_FILE"
else
    printf '%s=%s\n' "$ENV_KEY" "$SECRET_VALUE" > "$TEMP_FILE"
fi

chmod 600 "$TEMP_FILE"
mv "$TEMP_FILE" "$ENV_FILE"
trap - EXIT
unset SECRET_VALUE

echo "Updated $ENV_KEY in $ENV_FILE."
echo "Restart all Airflow components so they use the same secret."
