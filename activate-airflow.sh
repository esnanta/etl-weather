#!/usr/bin/env bash
# source activate-airflow.sh

_AIRFLOW_QUIET=0
for _airflow_arg in "$@"; do
    case "$_airflow_arg" in
        -q|--quiet)
            _AIRFLOW_QUIET=1
            ;;
        -h|--help)
            echo "Usage: source activate-airflow.sh [-q|--quiet]"
            return 0 2>/dev/null || exit 0
            ;;
        *)
            echo "ERROR: Unknown option '$_airflow_arg'." >&2
            echo "Usage: source activate-airflow.sh [-q|--quiet]" >&2
            return 1 2>/dev/null || exit 1
            ;;
    esac
done
unset _airflow_arg

_airflow_info() {
    [[ $_AIRFLOW_QUIET -eq 1 ]] && return 0
    echo "$@"
}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
    _AIRFLOW_ALLEXPORT_WAS_SET=0
    [[ $- == *a* ]] && _AIRFLOW_ALLEXPORT_WAS_SET=1

    set -a
    # shellcheck disable=SC1091
    if ! source "$PROJECT_ROOT/.env"; then
        [[ $_AIRFLOW_ALLEXPORT_WAS_SET -eq 0 ]] && set +a
        unset _AIRFLOW_ALLEXPORT_WAS_SET
        echo "ERROR: Failed to load $PROJECT_ROOT/.env." >&2
        return 1 2>/dev/null || exit 1
    fi

    [[ $_AIRFLOW_ALLEXPORT_WAS_SET -eq 0 ]] && set +a
    unset _AIRFLOW_ALLEXPORT_WAS_SET
fi

if [[ -z "${APP_ENV+x}" || -z "${APP_ENV//[[:space:]]/}" ]]; then
    echo "WARNING: APP_ENV is required." >&2
    echo "Set APP_ENV=dev or APP_ENV=prod in $PROJECT_ROOT/.env before starting the application." >&2
    return 1 2>/dev/null || exit 1
fi

case "${APP_ENV,,}" in
    dev|prod)
        export APP_ENV="${APP_ENV,,}"
        ;;
    *)
        echo "WARNING: Invalid APP_ENV='${APP_ENV}'." >&2
        echo "Set APP_ENV=dev or APP_ENV=prod in $PROJECT_ROOT/.env before starting the application." >&2
        return 1 2>/dev/null || exit 1
        ;;
esac

echo "Application environment | APP_ENV=$APP_ENV"
echo "Application environment | ERA5_CAMPAIGN_SCHEDULE_ENABLED=${ERA5_CAMPAIGN_SCHEDULE_ENABLED:-}"
echo "Application environment | ERA5_CAMPAIGN_SCHEDULE_CADENCE=${ERA5_CAMPAIGN_SCHEDULE_CADENCE:-}"
echo "Application environment | CDS_MIN_SUBMISSION_INTERVAL_SECONDS=${CDS_MIN_SUBMISSION_INTERVAL_SECONDS:-}"

# shellcheck disable=SC1091
source "$PROJECT_ROOT/myvenv/bin/activate"

AIRFLOW_JWT_SECRET_FILE="$PROJECT_ROOT/airflow-jwt-key.txt"

if [[ ! -s "$AIRFLOW_JWT_SECRET_FILE" ]]; then
    echo "ERROR: Missing or empty $AIRFLOW_JWT_SECRET_FILE." >&2
    echo "Generate it with: ./generate-airflow-jwt-secret.sh" >&2
    return 1 2>/dev/null || exit 1
fi

if [[ "$(stat -c '%a' "$AIRFLOW_JWT_SECRET_FILE")" != "600" ]]; then
    echo "ERROR: $AIRFLOW_JWT_SECRET_FILE must have permission 600." >&2
    echo "Fix it with: chmod 600 airflow-jwt-key.txt" >&2
    return 1 2>/dev/null || exit 1
fi

# Ignore a legacy direct value that may still exist in .env. Airflow reads the
# shared secret from the protected file through its supported _CMD mechanism.
unset AIRFLOW__API_AUTH__JWT_SECRET
unset AIRFLOW__API_AUTH__JWT_SECRET_SECRET
printf -v _AIRFLOW_JWT_SECRET_FILE_SHELL '%q' "$AIRFLOW_JWT_SECRET_FILE"
export AIRFLOW__API_AUTH__JWT_SECRET_CMD="cat $_AIRFLOW_JWT_SECRET_FILE_SHELL"
unset _AIRFLOW_JWT_SECRET_FILE_SHELL

export AIRFLOW_HOME="$PROJECT_ROOT/orchestration"
export AIRFLOW_CONFIG="$AIRFLOW_HOME/airflow.cfg"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export PYTHONPATH="$PROJECT_ROOT"

_airflow_info "PROJECT_ROOT=$PROJECT_ROOT"
_airflow_info "VIRTUAL_ENV=$VIRTUAL_ENV"
_airflow_info "AIRFLOW_HOME=$AIRFLOW_HOME"
_airflow_info "AIRFLOW_CONFIG=$AIRFLOW_CONFIG"
_airflow_info "AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
_airflow_info "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
_airflow_info "AIRFLOW_JWT_SECRET_FILE=$AIRFLOW_JWT_SECRET_FILE"
_airflow_info "PYTHONPATH=$PYTHONPATH"

unset -f _airflow_info
unset _AIRFLOW_QUIET
