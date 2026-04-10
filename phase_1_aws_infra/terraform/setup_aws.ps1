
$ErrorActionPreference = "Stop"

Write-Host "AWS Credential Setup Helper" -ForegroundColor Cyan
Write-Host "---------------------------" -ForegroundColor Cyan

# Check if .aws directory exists
$AwsDir = Join-Path $env:USERPROFILE ".aws"
if (-not (Test-Path $AwsDir)) {
    Write-Host "Creating .aws directory at $AwsDir"
    New-Item -ItemType Directory -Force -Path $AwsDir | Out-Null
}

$CredentialsFile = Join-Path $AwsDir "credentials"
$ConfigFile = Join-Path $AwsDir "config"

# Prompt for credentials
$AccessKey = Read-Host "Enter AWS Access Key ID"
$SecretKey = Read-Host "Enter AWS Secret Access Key"
$Region = Read-Host "Enter Default Region (default: us-east-1)"

if ([string]::IsNullOrWhiteSpace($Region)) {
    $Region = "us-east-1"
}

# Write credentials file
$CredContent = @"
[default]
aws_access_key_id = $AccessKey
aws_secret_access_key = $SecretKey
"@

$CredContent | Out-File -FilePath $CredentialsFile -Encoding ascii
Write-Host "Credentials saved to $CredentialsFile" -ForegroundColor Green

# Write config file
$ConfigContent = @"
[default]
region = $Region
output = json
"@

$ConfigContent | Out-File -FilePath $ConfigFile -Encoding ascii
Write-Host "Config saved to $ConfigFile" -ForegroundColor Green

Write-Host "`nSetup complete! You can now run terraform." -ForegroundColor Green
