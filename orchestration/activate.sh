#!/usr/bin/env bash

set -euo pipefail

orchestration_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${orchestration_dir}/.." && pwd)"

export AIRFLOW_HOME="${orchestration_dir}"
export AIRFLOW_CONFIG="${orchestration_dir}/airflow.cfg"
export AIRFLOW__CORE__DAGS_FOLDER="${orchestration_dir}/dags"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${orchestration_dir}/airflow.db"
export PYTHONPATH="${project_root}${PYTHONPATH:+:${PYTHONPATH}}"

echo "AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "AIRFLOW_CONFIG=${AIRFLOW_CONFIG}"
