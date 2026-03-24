using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.S3
{
    public class S3JobData : IDataLakeJobData
    {
        // Add S3-specific configuration properties (e.g., BucketName, AccessKey, SecretKey, Region, etc.)
        public string BucketName { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string Region { get; set; }
        public string RootDirectoryPath { get; set; }
        public string FileSystemName => BucketName;
        public string DirectoryName => RootDirectoryPath;
        // Implement other IDataLakeJobData members as needed
    }
}
