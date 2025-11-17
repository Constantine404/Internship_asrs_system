@echo off

IF NOT EXIST ".venv" (
    echo Creating virtual environment...
    python -m venv .venv
    call .venv\Scripts\activate
    python -m pip install -r requirements.txt
) ELSE (
    call .venv\Scripts\activate
)

python main.py

pause