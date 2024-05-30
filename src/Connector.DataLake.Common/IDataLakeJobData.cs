namespace CluedIn.Connector.DataLake;

public interface IDataLakeJobData
{
    string ContainerName { get; }
    bool IsStreamCacheEnabled { get; }
    bool UseCurrentTimeForExport { get; }
    string StreamCacheConnectionString { get; }
    string OutputFormat { get; }
    string Schedule { get; }
}
