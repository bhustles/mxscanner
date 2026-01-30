@echo off
echo ============================================
echo   EMAIL DATABASE DASHBOARD
echo ============================================
echo.
echo   Windows Dashboard: http://localhost:5000
echo   (connects to Windows PostgreSQL)
echo.
echo   WSL Dashboard:     http://172.18.253.136:5000
echo   (run in WSL: python web_dashboard.py)
echo.
echo   Press Ctrl+C to stop the server
echo ============================================
echo.
call C:\Users\mdbar\miniconda3\condabin\conda.bat activate email_processing
python "%~dp0web_dashboard.py"
pause
