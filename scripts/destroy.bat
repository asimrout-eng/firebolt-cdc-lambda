@echo off
REM Batch destroy script for Windows
REM Firebolt CDC Lambda - AWS CDK Destroy

echo.
set /p CONFIRM="Destroy all resources? Type 'destroy' to confirm: "

if "%CONFIRM%"=="destroy" (
    cdk destroy
    echo.
    echo Resources destroyed
) else (
    echo.
    echo Cancelled
)



