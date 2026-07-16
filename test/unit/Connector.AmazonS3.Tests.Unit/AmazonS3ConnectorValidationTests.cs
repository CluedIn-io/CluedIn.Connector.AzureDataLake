using CluedIn.Connector.AmazonS3.Connector;

using Xunit;

namespace CluedIn.Connector.AmazonS3.Tests.Unit;

public class AmazonS3ConnectorValidationTests
{
    [Theory]
    [InlineData("my-bucket", true)]
    [InlineData("my.bucket.name", true)]
    [InlineData("mybucket123", true)]
    [InlineData("123bucket", true)]
    [InlineData("abc", true)] // minimum length 3
    [InlineData("a]b", false)] // invalid characters
    [InlineData("ab", false)] // too short
    [InlineData("", false)]
    [InlineData("UPPERCASE", false)]
    [InlineData("my_bucket", false)] // underscores not allowed
    [InlineData("my--bucket", false)] // consecutive hyphens
    [InlineData("-my-bucket", false)] // starts with hyphen
    [InlineData("my-bucket-", false)] // ends with hyphen
    [InlineData("my..bucket", false)] // consecutive dots
    public void BucketNameRegex_ValidatesCorrectly(string bucketName, bool expectedValid)
    {
        var isValid = AmazonS3Connector.BucketNameRegex.IsMatch(bucketName);
        Assert.Equal(expectedValid, isValid);
    }
}
