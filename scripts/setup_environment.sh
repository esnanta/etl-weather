#!/bin/bash

set -euo pipefail

# Prepare the local Python environment for this repository.
# chmod +x setup_environment.sh
# ./setup_environment.sh

SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIRECTORY/.." && pwd)"
VENV_NAME="myvenv"
PYTHON_EXECUTABLE="python3"
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"
VENV_DIRECTORY="$PROJECT_ROOT/$VENV_NAME"

if [ ! -d "$VENV_DIRECTORY" ]; then
    echo "Creating virtual environment at $VENV_DIRECTORY..."
    "$PYTHON_EXECUTABLE" -m venv "$VENV_DIRECTORY"
fi

echo "Activating virtual environment..."
# shellcheck disable=SC1091
source "$VENV_DIRECTORY/bin/activate"

if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing dependencies from $REQUIREMENTS_FILE..."
    pip install -r "$REQUIREMENTS_FILE"
fi

echo "Environment setup is complete."
