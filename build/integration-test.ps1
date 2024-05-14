param(
    [Parameter(Mandatory)]
	[ValidateNotNullOrEmpty()]
	[ValidateSet('SetUp','TearDown')]
	[string]$Action
)

function Check-Errors($operationName) {
	if ($?)	{
		Write-Host "Sucessfully performed '$operationName'."
	} else {
		throw "Failed to perform '$operationName'."
	}
}

function Set-Variable($key, $value) {
	Write-Host "Setting variable '$key'."
	# We set this so that we can run things locally if we want to.
	[Environment]::SetEnvironmentVariable($key, $value)
	
	# If we want to totally keep Azure DevOps stuff out of this file,
	# then we can create a powershell task in azure pipelines yaml, 
	# and read the test details file & set the variables there.
	# Alternatively, we can dot source the appropriate "adapter" for Set-Variable function
	Write-Host "##vso[task.setvariable variable=$key]$value"	
}

function Run-Setup() {
	docker run -d -e "ACCEPT_EULA=Y" --name "datalaketest" -p ":1433" mcr.microsoft.com/mssql/server:2022-latest
	$port = (docker inspect "datalaketest" | ConvertFrom-Json).NetworkSettings.Ports."1433/tcp".HostPort
	Set-Variable "ADL2_STREAMCACHE" "Data Source=localhost,$($port);Initial Catalog=DataStore.Db.StreamCache;User Id=sa;Password=yourStrong(!)Password;connection timeout=0;Max Pool Size=200;Pooling=True"
	Start-Sleep 60
	docker exec datalaketest /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -Q 'CREATE DATABASE [DataStore.Db.Streamcache]'
}

function Run-TearDown() {
	docker rm -f "datalaketest"
}

function Run() {
	switch ($Action) 
	{
		"TearDown" {
			Write-Host "Performing tear down."
			Run-TearDown
		}
		
		"SetUp" {
			Write-Host "Performing set up using location '$ResourceLocation'."
			Run-SetUp
		}
	}
}

Run