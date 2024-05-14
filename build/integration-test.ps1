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
	docker run -d -e "ACCEPT_EULA=Y" --name "datalaketest" mcr.microsoft.com/mssql/server:2022-latest
}

function Run-TearDown() {
	docker rm -f "atalaketest"
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