@echo off
setlocal
py -3.12 -m cws enroll-device
set "EXIT_CODE=%ERRORLEVEL%"
echo.
if not "%EXIT_CODE%"=="0" (
  echo Enrollment command exited with code %EXIT_CODE%.
) else (
  echo Enrollment command finished successfully.
)
pause
endlocal
exit /b %EXIT_CODE%
