#!/bin/bash
# Activation script for weather-dataset Python environment

echo "==================================================="
echo "Weather Dataset Project - Python Environment"
echo "==================================================="
echo ""
echo "Mengaktifkan virtual environment..."

# Change to project directory
cd "$(dirname "$0")"

# Activate virtual environment
source myvenv/bin/activate

echo "✓ Virtual environment aktif"
echo ""
echo "Python version: $(python --version)"
echo "Pip version: $(pip --version | cut -d' ' -f1-2)"
echo ""
echo "==================================================="
echo "Environment siap digunakan!"
echo ""
echo "Untuk deaktivasi, ketik: deactivate"
echo "==================================================="
echo ""

# Start a new shell with the activated environment
exec $SHELL
