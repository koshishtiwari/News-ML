@echo off
REM setup_and_run.bat
REM Script to automate Python environment setup and run the news app

echo ============================================================
echo NEWS AGGREGATION SYSTEM SETUP
echo ============================================================

REM Check if virtual environment exists
if not exist .venv (
    echo Creating virtual environment...
    python -m venv .venv
) else (
    echo Virtual environment already exists.
)

echo Activating virtual environment...
call .venv\Scripts\activate

echo Installing dependencies...
pip install -r requirements.txt

echo ============================================================
echo STARTING APPLICATION
echo ============================================================
python main.py --monitor --api

echo ============================================================
echo Press any key to exit...
pause > nul