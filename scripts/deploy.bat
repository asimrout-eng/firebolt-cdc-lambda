@echo off
REM Batch deployment script for Windows
REM Firebolt CDC Lambda - AWS CDK Deployment

echo.
echo ============================================
echo   Deploying Firebolt CDC Lambda
echo ============================================
echo.

REM Check .env file
if not exist ".env" (
    echo WARNING: .env not found! Copying from .env.example...
    copy ".env.example" ".env"
    echo.
    echo ERROR: Edit .env and add your Firebolt credentials!
    exit /b 1
)

REM Install CDK dependencies
echo Installing CDK dependencies...
pip install -r requirements.txt

REM Build Lambda layer
echo Building Lambda layer...
if not exist "lambda-layer\python" mkdir "lambda-layer\python"
pip install -r lambda\requirements.txt -t lambda-layer\python\ --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.11

REM Get AWS Account ID
for /f %%i in ('aws sts get-caller-identity --query Account --output text') do set AWS_ACCOUNT=%%i

REM Bootstrap CDK
echo Bootstrapping CDK...
cdk bootstrap aws://%AWS_ACCOUNT%/ap-south-1

REM Deploy
echo Deploying stack...
cdk deploy --require-approval never

echo.
echo ============================================
echo   Deployment Complete!
echo ============================================
echo.
echo Next: Upload tables_keys.json:
echo   aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json
echo.



