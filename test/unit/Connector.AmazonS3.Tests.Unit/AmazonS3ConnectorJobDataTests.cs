using System.Collections.Generic;

using Newtonsoft.Json;

using Xunit;

namespace CluedIn.Connector.AmazonS3.Tests.Unit;

public class AmazonS3ConnectorConfigurationTests
{
    [Fact]
    public void AmazonS3ConnectorConfiguration_CouldBeDeserialized()
    {
        var configString = """{"AccessKey":"AKIAIOSFODNN7EXAMPLE","SecretKey":"***","BucketName":"my-bucket","Region":"us-east-1","DirectoryName":"myDir","ContainerName":"TargetContainer","Configurations":{"AccessKey":"AKIAIOSFODNN7EXAMPLE","SecretKey":"***","BucketName":"my-bucket","Region":"us-east-1","DirectoryName":"myDir","firstTime":true},"CrawlType":0,"TargetHost":null,"TargetCredentials":null,"TargetApiKey":null,"LastCrawlFinishTime":"0001-01-01T00:00:00+00:00","LastestCursors":null,"IsFirstCrawl":false,"ExpectedTaskCount":0,"IgnoreNextCrawl":false,"ExpectedStatistics":null,"ExpectedTime":"00:00:00","ExpectedData":0,"Errors":null}""";
        var configuration = JsonConvert.DeserializeObject<AmazonS3ConnectorConfiguration>(configString);

        Assert.NotNull(configuration);
        Assert.Equal("AKIAIOSFODNN7EXAMPLE", configuration.AccessKey);
        Assert.Equal("my-bucket", configuration.BucketName);
    }

    [Fact]
    public void AmazonS3ConnectorConfiguration_Properties_AreCorrectlyMapped()
    {
        var configurations = new Dictionary<string, object>
        {
            { AmazonS3ConfigurationConstants.AccessKey, "AKIAIOSFODNN7EXAMPLE" },
            { AmazonS3ConfigurationConstants.SecretKey, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" },
            { AmazonS3ConfigurationConstants.BucketName, "my-test-bucket" },
            { AmazonS3ConfigurationConstants.Region, "eu-west-1" },
            { AmazonS3ConfigurationConstants.DirectoryName, "exports/data" },
        };

        var configuration = new AmazonS3ConnectorConfiguration(configurations, "testContainer");

        Assert.Equal("AKIAIOSFODNN7EXAMPLE", configuration.AccessKey);
        Assert.Equal("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", configuration.SecretKey);
        Assert.Equal("my-test-bucket", configuration.BucketName);
        Assert.Equal("eu-west-1", configuration.Region);
        Assert.Equal("exports/data", configuration.DirectoryName);
        Assert.Equal("my-test-bucket", configuration.FileSystemName);
        Assert.Equal("exports/data", configuration.RootDirectoryPath);
    }

    [Fact]
    public void AmazonS3ConnectorConfiguration_WhenDirectoryNameIsNull_RootDirectoryPathIsEmpty()
    {
        var configurations = new Dictionary<string, object>
        {
            { AmazonS3ConfigurationConstants.AccessKey, "key" },
            { AmazonS3ConfigurationConstants.SecretKey, "secret" },
            { AmazonS3ConfigurationConstants.BucketName, "bucket" },
            { AmazonS3ConfigurationConstants.Region, "us-east-1" },
        };

        var configuration = new AmazonS3ConnectorConfiguration(configurations);

        Assert.Equal(string.Empty, configuration.RootDirectoryPath);
    }

    [Fact]
    public void AmazonS3ConnectorConfiguration_Equals_WhenSameValues_ReturnsTrue()
    {
        var config1 = new Dictionary<string, object>
        {
            { AmazonS3ConfigurationConstants.AccessKey, "key" },
            { AmazonS3ConfigurationConstants.SecretKey, "secret" },
            { AmazonS3ConfigurationConstants.BucketName, "bucket" },
            { AmazonS3ConfigurationConstants.Region, "us-east-1" },
            { AmazonS3ConfigurationConstants.DirectoryName, "dir" },
        };
        var config2 = new Dictionary<string, object>(config1);

        var configuration1 = new AmazonS3ConnectorConfiguration(config1);
        var configuration2 = new AmazonS3ConnectorConfiguration(config2);

        Assert.True(configuration1.Equals(configuration2));
        Assert.Equal(configuration1.GetHashCode(), configuration2.GetHashCode());
    }

    [Fact]
    public void AmazonS3ConnectorConfiguration_Equals_WhenDifferentValues_ReturnsFalse()
    {
        var config1 = new Dictionary<string, object>
        {
            { AmazonS3ConfigurationConstants.AccessKey, "key1" },
            { AmazonS3ConfigurationConstants.SecretKey, "secret" },
            { AmazonS3ConfigurationConstants.BucketName, "bucket" },
            { AmazonS3ConfigurationConstants.Region, "us-east-1" },
        };
        var config2 = new Dictionary<string, object>
        {
            { AmazonS3ConfigurationConstants.AccessKey, "key2" },
            { AmazonS3ConfigurationConstants.SecretKey, "secret" },
            { AmazonS3ConfigurationConstants.BucketName, "bucket" },
            { AmazonS3ConfigurationConstants.Region, "us-east-1" },
        };

        var configuration1 = new AmazonS3ConnectorConfiguration(config1);
        var configuration2 = new AmazonS3ConnectorConfiguration(config2);

        Assert.False(configuration1.Equals(configuration2));
    }
}
