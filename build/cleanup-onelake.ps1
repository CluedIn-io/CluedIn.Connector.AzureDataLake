param(
    # LakehouseTables: deletes all the tables of the lakehouse ONELAKE_ITEMNAME of the workspace ONELAKE_WORKSPACENAME, with the
    # service principal of the OneLake integration tests (ONELAKE_TENANTID, ONELAKE_CLIENTID, ONELAKE_CLIENTSECRET).
    #
    # LakehouseFiles: deletes everything under Files of the same lakehouse (the OneLake integration tests write there), with the same
    # service principal.
    #
    # MirroredDatabaseTables: deletes all the files under Tables of the mirrored database FABRICOPENMIRRORING_MIRROREDDATABASENAME
    # of the workspace ONELAKE_WORKSPACENAME, with the service principal of the Open Mirroring integration tests
    # (FABRICOPENMIRRORING_TENANTID, FABRICOPENMIRRORING_CLIENTID, FABRICOPENMIRRORING_CLIENTSECRET).
    [Parameter(Mandatory)]
    [ValidateSet('LakehouseTables', 'LakehouseFiles', 'MirroredDatabaseTables')]
    [string]$Target,

    # Lists what would be deleted, and deletes nothing.
    [switch]$DryRun,

    # For a test against something that is not OneLake: the endpoints of OneLake (DFS) and of the sign in of Entra ID.
    [string]$OneLakeEndpoint = 'https://onelake.dfs.fabric.microsoft.com',
    [string]$LoginEndpoint = 'https://login.microsoftonline.com'
)

$ErrorActionPreference = 'Stop'

function Get-RequiredVariable([string]$name) {
    $value = [Environment]::GetEnvironmentVariable($name)

    # A variable that is not defined in the pipeline is left as it is written in the step ($(NAME)), which is not a value
    if ([string]::IsNullOrWhiteSpace($value) -or $value -match '^\$\(.*\)$') {
        throw "$name is required."
    }

    return $value.Trim()
}

# A workspace or item name ends up in a path that is deleted recursively, so it must be a name and not a path.
function Assert-IsAName([string]$variableName, [string]$value) {
    if ($value -match '[/\\]' -or $value -eq '.' -or $value -eq '..') {
        throw "$variableName is not a name: '$value'."
    }
}

switch ($Target) {
    'LakehouseTables' {
        $workspace = Get-RequiredVariable 'ONELAKE_WORKSPACENAME'
        $item = Get-RequiredVariable 'ONELAKE_ITEMNAME'
        Assert-IsAName 'ONELAKE_ITEMNAME' $item
        $directory = "$item.Lakehouse/Tables"
        $tenantId = Get-RequiredVariable 'ONELAKE_TENANTID'
        $clientId = Get-RequiredVariable 'ONELAKE_CLIENTID'
        $clientSecret = Get-RequiredVariable 'ONELAKE_CLIENTSECRET'
    }
    'LakehouseFiles' {
        $workspace = Get-RequiredVariable 'ONELAKE_WORKSPACENAME'
        $item = Get-RequiredVariable 'ONELAKE_ITEMNAME'
        Assert-IsAName 'ONELAKE_ITEMNAME' $item
        $directory = "$item.Lakehouse/Files"
        $tenantId = Get-RequiredVariable 'ONELAKE_TENANTID'
        $clientId = Get-RequiredVariable 'ONELAKE_CLIENTID'
        $clientSecret = Get-RequiredVariable 'ONELAKE_CLIENTSECRET'
    }
    'MirroredDatabaseTables' {
        $workspace = Get-RequiredVariable 'ONELAKE_WORKSPACENAME'
        $item = Get-RequiredVariable 'FABRICOPENMIRRORING_MIRROREDDATABASENAME'
        Assert-IsAName 'FABRICOPENMIRRORING_MIRROREDDATABASENAME' $item
        $directory = "$item.MountedRelationalDatabase/Tables"
        $tenantId = Get-RequiredVariable 'FABRICOPENMIRRORING_TENANTID'
        $clientId = Get-RequiredVariable 'FABRICOPENMIRRORING_CLIENTID'
        $clientSecret = Get-RequiredVariable 'FABRICOPENMIRRORING_CLIENTSECRET'
    }
}
Assert-IsAName 'ONELAKE_WORKSPACENAME' $workspace

$OneLakeEndpoint = $OneLakeEndpoint.TrimEnd('/')
$LoginEndpoint = $LoginEndpoint.TrimEnd('/')

function Get-AccessToken {
    $response = Invoke-RestMethod -Method Post -Uri "$LoginEndpoint/$tenantId/oauth2/v2.0/token" -ContentType 'application/x-www-form-urlencoded' -Body @{
        client_id     = $clientId
        client_secret = $clientSecret
        scope         = 'https://storage.azure.com/.default'
        grant_type    = 'client_credentials'
    }

    return $response.access_token
}

function ConvertTo-UrlPath([string]$path) {
    return ($path -split '/' | ForEach-Object { [Uri]::EscapeDataString($_) }) -join '/'
}

# The paths directly under the directory (with the directory in them), or nothing when the directory is not there.
function Get-ChildPaths([hashtable]$headers) {
    $childPaths = @()
    $continuation = $null
    do {
        $uri = "$OneLakeEndpoint/$(ConvertTo-UrlPath $workspace)?resource=filesystem&recursive=false&directory=$([Uri]::EscapeDataString($directory))"
        if ($continuation) {
            $uri += "&continuation=$([Uri]::EscapeDataString($continuation))"
        }

        try {
            $response = Invoke-WebRequest -Method Get -Uri $uri -Headers $headers -UseBasicParsing
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                return @()
            }

            throw
        }

        $listing = $response.Content | ConvertFrom-Json
        $childPaths += @($listing.paths | Where-Object { $_ } | ForEach-Object { $_.name })

        $continuation = $null
        if ($response.Headers['x-ms-continuation']) {
            $continuation = @($response.Headers['x-ms-continuation'])[0]
        }
    } while ($continuation)

    return $childPaths
}

$headers = @{
    Authorization = "Bearer $(Get-AccessToken)"
    'x-ms-version' = '2023-11-03'
}

$childPaths = Get-ChildPaths $headers
Write-Host "$($childPaths.Count) item(s) under '$directory' in workspace '$workspace'."

$deleted = 0
foreach ($childPath in $childPaths) {
    # What is deleted is under the directory, and nothing else
    if (-not $childPath.StartsWith("$directory/", [StringComparison]::Ordinal)) {
        throw "Refusing to delete '$childPath', it is not under '$directory'."
    }

    if ($DryRun) {
        Write-Host "  would delete $childPath"
        continue
    }

    Invoke-RestMethod -Method Delete -Uri "$OneLakeEndpoint/$(ConvertTo-UrlPath $workspace)/$(ConvertTo-UrlPath $childPath)?recursive=true" -Headers $headers | Out-Null
    Write-Host "  deleted $childPath"
    $deleted++
}

Write-Host "Deleted $deleted item(s)."
