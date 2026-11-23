param(
    # The containers (file systems) the integration tests create all start with this. Only these are deleted.
    [ValidateNotNullOrEmpty()]
    [string]$ContainerPrefix = 'xunit-fs-',

    # Lists what would be deleted, and deletes nothing. The same when CLEANUP_DRY_RUN is true.
    [switch]$DryRun,

    # For a storage emulator (Azurite): the blob endpoint to use instead of the one of the account.
    [string]$BlobEndpoint
)

$ErrorActionPreference = 'Stop'

if ($env:CLEANUP_DRY_RUN -eq 'true') {
    $DryRun = [switch]$true
}

# The integration tests' storage account. The Azure CLI reads these two.
# A variable that is not defined in the pipeline is left as it is written in the step ($(NAME)), which is not a value
$isMissing = { param($value) [string]::IsNullOrWhiteSpace($value) -or $value -match '^\$\(.*\)$' }
if ((& $isMissing $env:ADL2_ACCOUNTNAME) -or (& $isMissing $env:ADL2_ACCOUNTKEY)) {
    throw 'ADL2_ACCOUNTNAME and ADL2_ACCOUNTKEY are required.'
}
$env:AZURE_STORAGE_ACCOUNT = $env:ADL2_ACCOUNTNAME
$env:AZURE_STORAGE_KEY = $env:ADL2_ACCOUNTKEY
$endpointArguments = if ($BlobEndpoint) { @('--blob-endpoint', $BlobEndpoint) } else { @() }

function Get-Containers {
    $names = az storage container list --prefix $ContainerPrefix --query '[].name' --output tsv @endpointArguments
    if ($LASTEXITCODE -ne 0) { throw 'Unable to list the containers.' }

    # The prefix is filtered by the service, and checked again here before anything is deleted.
    return @($names | Where-Object { $_ -and $_.StartsWith($ContainerPrefix, [StringComparison]::Ordinal) })
}

# A listing is limited to 5000 containers, so list again until none are left (or a pass deleted nothing).
$deleted = 0
for ($pass = 1; $pass -le 20; $pass++) {
    $containers = Get-Containers
    Write-Host "Pass ${pass}: $($containers.Count) container(s) start with '$ContainerPrefix' in storage account '$($env:ADL2_ACCOUNTNAME)'."
    if ($containers.Count -eq 0) { break }

    if ($DryRun) {
        $containers | Select-Object -First 20 | ForEach-Object { Write-Host "  would delete $_" }
        break
    }

    $failures = $containers | ForEach-Object -ThrottleLimit 16 -Parallel {
        # Deleting a container that is already gone is fine.
        $arguments = @('storage', 'container', 'delete', '--name', $_, '--output', 'none')
        if ($using:BlobEndpoint) { $arguments += @('--blob-endpoint', $using:BlobEndpoint) }
        az @arguments 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { $_ }
    }

    $deleted += $containers.Count - @($failures).Count
    if (@($failures).Count -eq $containers.Count) {
        throw "Unable to delete any of the $($containers.Count) containers."
    }
}

Write-Host "Deleted $deleted container(s)."
