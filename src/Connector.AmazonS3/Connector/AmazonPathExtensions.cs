using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AmazonS3.Connector;

internal static class AmazonPathExtensions
{
    public static string GetKey(this FilePath filePath)
    {
        return filePath.FullPath.TrimStart('/');
    }
    public static string GetPrefix(this DirectoryPath directoryPath)
    {
        return directoryPath.Path.TrimStart('/');
    }

    public static string GetS3Url(this FilePath filePath, string bucketName)
    {
        return $"s3://{bucketName}/{filePath.GetKey()}";
    }
}
