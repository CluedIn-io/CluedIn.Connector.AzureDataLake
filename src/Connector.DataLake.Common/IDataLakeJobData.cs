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
