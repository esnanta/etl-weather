#!/usr/bin/env bash
# source activate-airflow.sh

source myvenv/bin/activate

if [[ -z "${AIRFLOW__API_AUTH__JWT_SECRET:-}" ]]; then
    echo "ERROR: AIRFLOW__API_AUTH__JWT_SECRET is not set." >&2
    echo "Generate it once with: openssl rand -hex 32" >&2
    echo "Then store it in the untracked .env file before starting Airflow." >&2
    return 1 2>/dev/null || exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
