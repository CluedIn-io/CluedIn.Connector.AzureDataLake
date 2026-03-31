using System;
using System.Collections.Generic;

using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AmazonS3;

internal class AmazonS3ConnectorConfiguration : StorageConfigurationBase
{
    public AmazonS3ConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccessKey => GetConfigurationValue(AmazonS3ConfigurationConstants.AccessKey) as string;
    public string SecretKey => GetConfigurationValue(AmazonS3ConfigurationConstants.SecretKey) as string;
    public string BucketName => GetConfigurationValue(AmazonS3ConfigurationConstants.BucketName) as string;
    public string Region => GetConfigurationValue(AmazonS3ConfigurationConstants.Region) as string;
    public string DirectoryName => GetConfigurationValue(AmazonS3ConfigurationConstants.DirectoryName) as string;

    public override string FileSystemName => BucketName;
    public override string RootDirectoryPath => DirectoryName ?? string.Empty;

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(AccessKey);
        hash.Add(SecretKey);
        hash.Add(BucketName);
        hash.Add(Region);
        hash.Add(DirectoryName);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as AmazonS3ConnectorConfiguration);
    }

    public bool Equals(AmazonS3ConnectorConfiguration other)
    {
        return other != null &&
            AccessKey == other.AccessKey &&
            SecretKey == other.SecretKey &&
            BucketName == other.BucketName &&
            Region == other.Region &&
            RootDirectoryPath == other.RootDirectoryPath &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
