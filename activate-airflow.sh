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

if [[ -z "${AIRFLOW__API_AUTH__JWT_SECRET:-}" ]]; then
    echo "ERROR: AIRFLOW__API_AUTH__JWT_SECRET is not set." >&2
    echo "Generate it once with: openssl rand -hex 32" >&2
    echo "Then store it in the untracked .env file before starting Airflow." >&2
    return 1 2>/dev/null || exit 1
fi

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
echo "PYTHONPATH=$PYTHONPATH"
