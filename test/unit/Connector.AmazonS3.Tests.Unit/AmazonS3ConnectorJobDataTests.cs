using System.Collections.Generic;

using Newtonsoft.Json;

using Xunit;

namespace CluedIn.Connector.AmazonS3.Tests.Unit;

public class AmazonS3ConnectorJobDataTests
{
    [Fact]
    public void AmazonS3ConnectorJobData_CouldBeDeserialized()
    {
        var configString = """{"AccessKey":"AKIAIOSFODNN7EXAMPLE","SecretKey":"***","BucketName":"my-bucket","Region":"us-east-1","DirectoryName":"myDir","ContainerName":"TargetContainer","Configurations":{"AccessKey":"AKIAIOSFODNN7EXAMPLE","SecretKey":"***","BucketName":"my-bucket","Region":"us-east-1","DirectoryName":"myDir","firstTime":true},"CrawlType":0,"TargetHost":null,"TargetCredentials":null,"TargetApiKey":null,"LastCrawlFinishTime":"0001-01-01T00:00:00+00:00","LastestCursors":null,"IsFirstCrawl":false,"ExpectedTaskCount":0,"IgnoreNextCrawl":false,"ExpectedStatistics":null,"ExpectedTime":"00:00:00","ExpectedData":0,"Errors":null}""";
        var jobData = JsonConvert.DeserializeObject<AmazonS3ConnectorJobData>(configString);

        Assert.NotNull(jobData);
        Assert.Equal("AKIAIOSFODNN7EXAMPLE", jobData.AccessKey);
        Assert.Equal("my-bucket", jobData.BucketName);
    }

    [Fact]
    public void AmazonS3ConnectorJobData_Properties_AreCorrectlyMapped()
    {
        var configurations = new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, "AKIAIOSFODNN7EXAMPLE" },
            { AmazonS3Constants.SecretKey, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" },
            { AmazonS3Constants.BucketName, "my-test-bucket" },
            { AmazonS3Constants.Region, "eu-west-1" },
            { AmazonS3Constants.DirectoryName, "exports/data" },
        };

        var jobData = new AmazonS3ConnectorJobData(configurations, "testContainer");

        Assert.Equal("AKIAIOSFODNN7EXAMPLE", jobData.AccessKey);
        Assert.Equal("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", jobData.SecretKey);
        Assert.Equal("my-test-bucket", jobData.BucketName);
        Assert.Equal("eu-west-1", jobData.Region);
        Assert.Equal("exports/data", jobData.DirectoryName);
        Assert.Equal("my-test-bucket", jobData.FileSystemName);
        Assert.Equal("exports/data", jobData.RootDirectoryPath);
    }

    [Fact]
    public void AmazonS3ConnectorJobData_WhenDirectoryNameIsNull_RootDirectoryPathIsEmpty()
    {
        var configurations = new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, "key" },
            { AmazonS3Constants.SecretKey, "secret" },
            { AmazonS3Constants.BucketName, "bucket" },
            { AmazonS3Constants.Region, "us-east-1" },
        };

        var jobData = new AmazonS3ConnectorJobData(configurations);

        Assert.Equal(string.Empty, jobData.RootDirectoryPath);
    }

    [Fact]
    public void AmazonS3ConnectorJobData_Equals_WhenSameValues_ReturnsTrue()
    {
        var config1 = new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, "key" },
            { AmazonS3Constants.SecretKey, "secret" },
            { AmazonS3Constants.BucketName, "bucket" },
            { AmazonS3Constants.Region, "us-east-1" },
            { AmazonS3Constants.DirectoryName, "dir" },
        };
        var config2 = new Dictionary<string, object>(config1);

        var jobData1 = new AmazonS3ConnectorJobData(config1);
        var jobData2 = new AmazonS3ConnectorJobData(config2);

        Assert.True(jobData1.Equals(jobData2));
        Assert.Equal(jobData1.GetHashCode(), jobData2.GetHashCode());
    }

    [Fact]
    public void AmazonS3ConnectorJobData_Equals_WhenDifferentValues_ReturnsFalse()
    {
        var config1 = new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, "key1" },
            { AmazonS3Constants.SecretKey, "secret" },
            { AmazonS3Constants.BucketName, "bucket" },
            { AmazonS3Constants.Region, "us-east-1" },
        };
        var config2 = new Dictionary<string, object>
        {
            { AmazonS3Constants.AccessKey, "key2" },
            { AmazonS3Constants.SecretKey, "secret" },
            { AmazonS3Constants.BucketName, "bucket" },
            { AmazonS3Constants.Region, "us-east-1" },
        };

        var jobData1 = new AmazonS3ConnectorJobData(config1);
        var jobData2 = new AmazonS3ConnectorJobData(config2);

        Assert.False(jobData1.Equals(jobData2));
    }
}
