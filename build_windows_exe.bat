\
@echo off
setlocal
cd /d "%~dp0"

py -m pip install --upgrade pip
py -m pip install -r requirements-exe.txt

py -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --name AcademicPaperOrganizer ^
  --windowed ^
  --paths src ^
  --collect-all fitz ^
  --hidden-import academic_paper_organizer ^
  --hidden-import academic_paper_organizer.core ^
  --hidden-import academic_paper_organizer.gui ^
  src\academic_paper_organizer\gui.py

echo.
echo Build complete.
echo EXE path: dist\AcademicPaperOrganizer\AcademicPaperOrganizer.exe
pause
