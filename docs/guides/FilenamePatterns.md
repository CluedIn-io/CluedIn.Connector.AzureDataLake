# Filename patterns
The connectors allow setting of filename pattern to customize output file names.
The following filename patterns are supported
* {DataTime} and {DataTime:formatString}
  * DataTime is in UTC
  * formatString accepts formats available to C# DateTime.ToString() Please refer to [here](https://learn.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings) and [here](https://learn.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings) for more information.
  * When using {DataTime} without formatString, it defaults to "o" format. Example values: `2024-07-05T03:02:57.2612933Z`.
* {StreamId} and {DataTime:formatString} 
  * formatString accepts formats available to C# Guid.ToString() method. Please refer to [here](https://learn.microsoft.com/en-us/dotnet/api/system.guid.tostring?view=net-8.0) for more information.
  * When using {StreamId} without formatString, it defaults to "D" format. Example values: `ba4afc12-f6dc-4394-b9d5-68f6dacf3b3b`.
* {OutputFormat} and {OutputFormat:formatString}
  * formatString accepts the following:-
    * ToUpperInvariant
    * ToUpper (equivalent to ToUpperInvariant)
    * ToLower
    * ToUpperInvariant (equivalent to ToLowerInvariant)
  * When using {OutputFormat} without formatString, no extra formatting is performed. Example values: `csv`, `parquet`, `json`.
* {ContainerName}
  * this will use the value in the Storage Name of the Stream.

## Example filename patterns
| Filename Pattern                                         | Example output                                                            |
|----------------------------------------------------------|---------------------------------------------------------------------------|
| {StreamId:N}_{DataTime:yyyyMMddHHmmss}.{OutputFormat}    | ba4afc12f6dc4394b9d568f6dacf3b3b_20240705030355.parquet                   |
| {StreamId}_{DataTime}.{OutputFormat:ToUpper}             | ba4afc12-f6dc-4394-b9d5-68f6dacf3b3b_2024-07-05T03:02:57.2612933Z.PARQUET |
| {ContainerName}_{DataTime:yyyyMMddHHmmss}.{OutputFormat} | CustomerRecord_20240705030355.parquet                                     |

# Default file names
Default file names are as follows:
* AzureDataLakeConnector: {StreamId:D}_{DataTime:yyyyMMddHHmmss}.{OutputFormat}
* OneLakeConnector: {StreamId:N}_{DataTime:yyyyMMddHHmmss}.{OutputFormat}