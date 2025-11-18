# PowerShell destroy script for Windows
# Firebolt CDC Lambda - AWS CDK Destroy

Write-Host ""
Write-Host "⚠️  Destroy all resources?" -ForegroundColor Yellow
$CONFIRM = Read-Host "Type 'destroy' to confirm"

if ($CONFIRM -eq "destroy") {
    cdk destroy
    Write-Host "✅ Resources destroyed" -ForegroundColor Green
} else {
    Write-Host "❌ Cancelled" -ForegroundColor Red
}



