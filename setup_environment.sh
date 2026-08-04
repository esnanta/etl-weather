#!/bin/bash
# Setup script for the weather-dataset Python environment
# Execute
#     chmod +x setup_environment.sh
#    ./setup_environment.sh

# --- Configuration ---
VENV_NAME="myvenv"
REQUIREMENTS_FILE="requirements.txt"
PYTHON_VERSION="python3"

# --- Helper Functions ---
print_header() {
    echo "==================================================="
    echo "  Weather Dataset Project - Environment Setup"
    echo "==================================================="
    echo ""
}

print_step() {
    echo "--- $1 ---"
}

print_success() {
    echo "✓ $1"
    echo ""
}

# --- Main Script ---
print_header

# 1. Create Virtual Environment
if [ ! -d "$VENV_NAME" ]; then
    print_step "Creating virtual environment..."
    $PYTHON_VERSION -m venv $VENV_NAME
    print_success "Virtual environment '$VENV_NAME' created."
else
    print_step "Virtual environment '$VENV_NAME' already exists."
    echo ""
fi

# 2. Activate Virtual Environment
print_step "Activating virtual environment..."
source "$VENV_NAME/bin/activate"
print_success "Virtual environment activated."

# 3. Freeze and Update Requirements (Optional)
read -p "Do you want to freeze the current environment's packages to $REQUIREMENTS_FILE? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Freezing requirements..."
    pip freeze > $REQUIREMENTS_FILE
    print_success "Requirements frozen to $REQUIREMENTS_FILE."
fi

# 4. Install Requirements
if [ -f "$REQUIREMENTS_FILE" ]; then
    print_step "Installing requirements from $REQUIREMENTS_FILE..."
    pip install -r $REQUIREMENTS_FILE
    print_success "Requirements installed."
else
    echo "Warning: $REQUIREMENTS_FILE not found. Skipping installation."
    echo ""
fi

# --- Summary ---
echo "==================================================="
echo "  Environment is ready!"
echo "==================================================="
echo "Python version: $(python --version)"
echo "Pip version: $(pip --version | cut -d' ' -f1-2)"
echo ""
echo "To deactivate, type: deactivate"
echo "==================================================="
echo ""

# Start a new shell with the activated environment
exec $SHELL
