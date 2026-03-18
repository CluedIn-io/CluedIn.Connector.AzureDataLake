namespace CluedIn.Connector.DataLake.Common;

public interface IDataLakeJobData
{
    string ContainerName { get; }
    bool IsStreamCacheEnabled { get; }
    bool UseCurrentTimeForExport { get; }
    string FileNamePattern { get; }
    string StreamCacheConnectionString { get; }
    string OutputFormat { get; }
    string Schedule { get; }
    bool ShouldWriteGuidAsString { get; }
    bool ShouldEscapeVocabularyKeys { get; }
    string CustomCron { get; }
    bool IsDeltaMode { get; }
    bool IsOverwriteEnabled { get; }
    bool IsArrayColumnsEnabled { get; }

    string FileSystemName { get; }

    string RootDirectoryPath { get; }
}

internal interface IAzureSharedKeyCredentialJobData
{
    string AccountName { get; }
    string AccountKey { get; }
    string StorageUri { get; }
}

internal interface IAzureServicePrincipalCredentialJobData
{
    string TenantId { get; }
    string ClientId { get; }
    string ClientSecret { get; }
    string AccountName { get; }
    string StorageUri { get; }
}
