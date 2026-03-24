using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.S3
{
    public class S3JobData : IDataLakeJobData
    {
        // S3-specific properties
        public string BucketName { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string Region { get; set; }
        public string RootDirectoryPath { get; set; }
        public string FileSystemName => BucketName;
        public string DirectoryName => RootDirectoryPath;

        // IDataLakeJobData required properties (implement with auto-properties or defaults for now)
        public string ContainerName { get; set; }
        public bool IsStreamCacheEnabled { get; set; }
        public bool UseCurrentTimeForExport { get; set; }
        public string FileNamePattern { get; set; }
        public string StreamCacheConnectionString { get; set; }
        public string OutputFormat { get; set; }
        public string Schedule { get; set; }
        public bool ShouldWriteGuidAsString { get; set; }
        public bool ShouldEscapeVocabularyKeys { get; set; }
        public string CustomCron { get; set; }
        public bool IsDeltaMode { get; set; }
        public bool IsOverwriteEnabled { get; set; }
        public bool IsArrayColumnsEnabled { get; set; }
    }
}
