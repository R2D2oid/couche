#!/bin/bash
# Run once to create the virtual environment for this project.
# Usage: bash setup_env.sh

module load python/3.11.5 scipy-stack/2024a StdEnv/2023 arrow/17.0.0

virtualenv --no-download --clear ENV
source ENV/bin/activate

pip install --no-index --upgrade pip
pip install --no-index -r requirements.txt

echo "Done. To activate in future sessions, run: source activate.sh"
