#!/bin/bash
# Setup script for the weather-dataset Python environment
# chmod +x setup_environment.sh
# ./setup_environment.sh

# Configuration
VENV_NAME="myvenv"
REQUIREMENTS_FILE="requirements.txt"
PYTHON_VERSION="python3"

# 1. Create Virtual Environment
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating virtual environment..."
    $PYTHON_VERSION -m venv $VENV_NAME
fi

# 2. Activate Virtual Environment
echo "Activating virtual environment..."
source "$VENV_NAME/bin/activate"

# 3. Install Requirements
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing requirements from $REQUIREMENTS_FILE..."
    pip install -r $REQUIREMENTS_FILE
fi

echo "Environment is ready!"
