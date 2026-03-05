param (
    [string]$BuildDir
)

$ErrorActionPreference = "Stop"

# --- 1. DEFINE & VALIDATE PATHS ---
# Check for environment variable, otherwise fallback to the relative path
$envPublishDir = $env:CLUEDIN_PUBLISH_DIR
$fallbackPath = Resolve-Path "$PSScriptRoot\..\CluedIn\Code\Server.Host\bin\Debug\publish" -ErrorAction SilentlyContinue

if ($envPublishDir -and (Test-Path $envPublishDir)) {
    $baseDir = $envPublishDir
    Write-Host "Using Publish Directory from Env Var: $baseDir" -ForegroundColor Cyan
}
elseif ($fallbackPath -and (Test-Path $fallbackPath)) {
    $baseDir = $fallbackPath.Path
    Write-Host "Env Var not found or invalid. Using fallback: $baseDir" -ForegroundColor Yellow
}
else {
    Write-Error "Could not locate the CluedIn publish directory. Please check your paths or set 'CLUEDIN_PUBLISH_DIR'."
    exit
}

# Define specific sub-paths based on the validated base directory
$publishDir = Join-Path $baseDir "ServerComponent"
$app = Join-Path $baseDir "CluedIn.Server.Host.dll"

# Final sanity check for the required DLL
if (-not (Test-Path $app)) {
    Write-Error "The application DLL was not found at: $app"
    exit
}

# --- 2. KILL RUNNING SERVICE ---
Write-Host "Checking for running CluedIn instances..." -ForegroundColor Cyan
$runningProcs = Get-Process dotnet -ErrorAction Ignore | Where-Object {
    $_.CommandLine -match 'CluedIn.Server.Host'
}
if ($runningProcs) {
    $runningProcs | Stop-Process -Force
    Write-Host "Terminated existing process." -ForegroundColor Yellow
    Start-Sleep -Seconds 1
}

# --- 3. COPY BUILD ARTIFACTS ---
if (Test-Path $BuildDir) {
    Get-ChildItem $BuildDir | ForEach-Object {
        Copy-Item $_.FullName $publishDir -Force
        Write-Host "  -> Copied: $($_.Name)" -ForegroundColor Green
    }
}

# --- 4. LOAD ENVIRONMENT SETTINGS ---
# Assuming launchSettings.json stays relative to the source code structure
$launchSettingsPath = "$PSScriptRoot\..\CluedIn\Code\Server.Host\Properties\launchSettings.json"

if (Test-Path $launchSettingsPath) {
    $settings = Get-Content -Raw -Path $launchSettingsPath | ConvertFrom-Json
    $profile = $settings.profiles."Server.Host"

    if ($profile -and $profile.environmentVariables) {
        Write-Host "Setting environment variables from 'Server.Host'..." -ForegroundColor Cyan

        $profile.environmentVariables.PSObject.Properties | ForEach-Object {
            Set-Item -Path "Env:$($_.Name)" -Value $_.Value
            Write-Host "  [SET] $($_.Name)" -ForegroundColor Gray
        }
    }
}
else {
    Write-Warning "launchSettings.json not found at $launchSettingsPath. Skipping env var injection."
}

# --- 5. RUN ---
cd $baseDir
Write-Host "Starting CluedIn..." -ForegroundColor Magenta
dotnet $app