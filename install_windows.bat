@echo off
cd /d %~dp0
python -m pip install --upgrade pip
python -m pip install .
echo.
echo Installation complete.
pause


echo Launch GUI: paper-organizer-gui
