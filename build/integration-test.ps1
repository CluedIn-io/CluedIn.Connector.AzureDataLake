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

function Run-Setup() {
	$password = "yourStrong(!)Password"
	$databaseName = "DataStore.Db.StreamCache"
	$containerName = "datalaketest"
	$sqlServerImage = "mcr.microsoft.com/mssql/server:2022-latest"
	docker run -d -e "ACCEPT_EULA=Y" --name $containerName -p ":1433" -e "MSSQL_SA_PASSWORD=$($password)" $sqlServerImage
	$port = ((docker inspect $containerName | convertfrom-json).NetworkSettings.Ports."1433/tcp" | Where-Object { $_.HostIp -eq '0.0.0.0'}).HostPort
	$connectionString = "Data Source=localhost,$($port);Initial Catalog=$($databaseName);User Id=sa;Password=$($password);connection timeout=0;Max Pool Size=200;Pooling=True"
	$connectionStringEncoded = [Convert]::ToBase64String([char[]]$connectionString)
	Set-Variable "ADL2_STREAMCACHE" $connectionStringEncoded
	Write-Host "##[command]docker logs $containerName -f"
	WaitFor { docker logs -f $containerName } "MS SQL Ready for requests"
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