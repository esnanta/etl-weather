#!/usr/bin/env bash
# source activate-airflow.sh

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

echo "PROJECT_ROOT=$PROJECT_ROOT"
echo "VIRTUAL_ENV=$VIRTUAL_ENV"
echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "AIRFLOW_CONFIG=$AIRFLOW_CONFIG"
echo "AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
echo "AIRFLOW_JWT_SECRET_FILE=$AIRFLOW_JWT_SECRET_FILE"
echo "PYTHONPATH=$PYTHONPATH"
