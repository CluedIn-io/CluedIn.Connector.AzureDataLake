param(
    [Parameter(Mandatory)]
	[ValidateNotNullOrEmpty()]
	[ValidateSet('SetUp','TearDown')]
	[string]$Action
)
function WaitFor {
	param(
		[scriptblock]$Command,
		[string]$Message
	)
	$job = Start-Job $Command
	do {
		$found = Receive-Job $job
		$found | Write-Host
	} until ($found  | Select-String $Message)
	Stop-Job $job
	Remove-Job $job
}

function Check-Errors($operationName) {
	if ($?)	{
		Write-Host "Sucessfully performed '$operationName'."
	} else {
		throw "Failed to perform '$operationName'."
	}
}

function Set-Variable($key, $value) {
	Write-Host "Setting variable '$key'. to '$value'."
	# We set this so that we can run things locally if we want to.
	[Environment]::SetEnvironmentVariable($key, $value)
	
	# If we want to totally keep Azure DevOps stuff out of this file,
	# then we can create a powershell task in azure pipelines yaml, 
	# and read the test details file & set the variables there.
	# Alternatively, we can dot source the appropriate "adapter" for Set-Variable function
	Write-Host "##vso[task.setvariable variable=$key]$value"	
}

function Get-ContainerName() {
	return "datalaketest"
}

function Run-Setup() {
	$databaseName = "DataStore.Db.StreamCache"
	$databaseHost = "localhost"
	$databaseUser = "sa"
	$databasePassword = "yourStrong(!)Password"
	$containerName = (Get-ContainerName)
	$sqlServerImage = "mcr.microsoft.com/mssql/server:2022-latest"
	
	Write-Host "##[command]docker run -d -e `"ACCEPT_EULA=Y`" --name $containerName -p `":1433`" -e `"MSSQL_SA_PASSWORD=$($databasePassword)`" $sqlServerImage"
	docker run -d -e "ACCEPT_EULA=Y" --name $containerName -p ":1433" -e "MSSQL_SA_PASSWORD=$($databasePassword)" $sqlServerImage
	$port = ((docker inspect $containerName | convertfrom-json).NetworkSettings.Ports."1433/tcp" | Where-Object { $_.HostIp -eq '0.0.0.0'}).HostPort
	$connectionString = "Data Source=$($databaseHost),$($port);Initial Catalog=$($databaseName);User Id=$($databaseUser);Password=$($databasePassword);connection timeout=0;Max Pool Size=200;Pooling=True"
	$connectionStringEncoded = [Convert]::ToBase64String([char[]]$connectionString)
	Set-Variable "ADL2_STREAMCACHE" $connectionStringEncoded
	Write-Host "##[command]docker logs -f $($containerName)"
	WaitFor { docker container logs --follow "datalaketest" } "SQL Server is now ready for client connections"
	
	# Wait for additional 5 seconds to make sure it's really ready
	Start-Sleep 5
	$createDatabaseCommand = "CREATE DATABASE [$databaseName]"
	Write-Host "##[command]docker exec $containerName /opt/mssql-tools18/bin/sqlcmd -S $databaseHost -U $databaseUser -P $databasePassword -C -Q $createDatabaseCommand"
	docker exec $containerName /opt/mssql-tools18/bin/sqlcmd -S $databaseHost -U $databaseUser -P $databasePassword -C -Q $createDatabaseCommand
}

function Run-TearDown() {
	docker rm -f (Get-ContainerName)
}

function Run() {
	switch ($Action) 
	{
		"TearDown" {
			Write-Host "Performing tear down."
			Run-TearDown
		}
		
		"SetUp" {
			Write-Host "Performing set up."
			Run-SetUp
		}
	}
}

Run