namespace CluedIn.Connector.FileStorage.Common;

public interface IStorageConfiguration
{
    string ContainerName { get; }

    /// <summary>
    /// When true, export options are configured on the connector. Otherwise they are configured on each stream.
    /// Defaults to true when not set.
    /// </summary>
    bool UseLegacyOptions { get; }

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

    string RootDirectoryPath { get; }
    bool IsSoftDelete { get; }
}
