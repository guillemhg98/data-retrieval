@echo off
REM Setup script for Windows - Creates directory structure
REM Usage: setup.bat

echo Setting up project directories...

REM Create data directory structure
mkdir data\demand_pipeline\state 2>nul
mkdir data\demand_pipeline\incremental 2>nul
mkdir data\demand_pipeline\finals 2>nul
mkdir data\diagnosis_pipeline\state 2>nul
mkdir data\diagnosis_pipeline\incremental 2>nul
mkdir data\diagnosis_pipeline\finals 2>nul
mkdir data\diagnosis_pipeline\selected_codes 2>nul
mkdir data\finals 2>nul
mkdir data\sample\input 2>nul
mkdir data\sample\output 2>nul
mkdir selections 2>nul

echo.
echo Project directories created successfully!
echo.
echo Next steps:
echo 1. Create virtual environment: python -m venv .venv
echo 2. Activate virtual environment: .venv\Scripts\activate
echo 3. Install requirements: pip install -r requirements.txt
echo 4. Validate sample mode: python run_pipeline.py --sample --all
echo 5. Copy and configure .env for production
echo.
echo Then run: python run_pipeline.py --help
pause
