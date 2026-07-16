using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Amazon.S3;

using CluedIn.Connector.AmazonS3.Connector;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace CluedIn.Connector.AmazonS3.Tests.Integration;

public partial class AmazonS3WriteStreamTests : IAsyncLifetime
{
    protected readonly ITestOutputHelper _testOutputHelper;
    private AmazonS3ConnectorConfiguration _configuration;
    private IAmazonS3 _s3Client;
    private readonly List<string> _createdKeys = new();

    public AmazonS3WriteStreamTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    protected ITestOutputHelper TestOutputHelper => _testOutputHelper;

    [Fact]
    public async Task WriteSmallFile_UsesSimplePutObject()
    {
        var key = $"{_configuration.RootDirectoryPath}/small-{Guid.NewGuid()}.txt";
        _createdKeys.Add(key);

        var content = "Hello, S3 integration test!";
        var bytes = Encoding.UTF8.GetBytes(content);

        await using (var stream = new AmazonS3WriteStream(NullLogger<AmazonS3WriteStream>.Instance, _s3Client, _configuration.BucketName, key))
        {
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }

        var response = await _s3Client.GetObjectAsync(_configuration.BucketName, key);
        using var reader = new StreamReader(response.ResponseStream);
        var actual = await reader.ReadToEndAsync();

        Assert.Equal(content, actual);
    }

    [Fact]
    public async Task WriteLargeFile_UsesMultipartUpload()
    {
        var key = $"{_configuration.RootDirectoryPath}/large-{Guid.NewGuid()}.bin";
        _createdKeys.Add(key);

        // Write > 5 MB to trigger multipart upload
        var partSize = 5 * 1024 * 1024;
        var totalSize = partSize + 1024; // slightly over one part
        var data = new byte[totalSize];
        new Random(42).NextBytes(data);

        await using (var stream = new AmazonS3WriteStream(NullLogger<AmazonS3WriteStream>.Instance, _s3Client, _configuration.BucketName, key))
        {
            // Write in chunks to simulate realistic usage
            var offset = 0;
            var chunkSize = 64 * 1024;
            while (offset < data.Length)
            {
                var count = Math.Min(chunkSize, data.Length - offset);
                await stream.WriteAsync(data, offset, count);
                offset += count;
            }
        }

        var response = await _s3Client.GetObjectAsync(_configuration.BucketName, key);
        using var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms);
        var actual = ms.ToArray();

        Assert.Equal(data.Length, actual.Length);
        Assert.Equal(data, actual);
    }

    [Fact]
    public async Task WriteEmptyFile_DoesNotThrow()
    {
        var key = $"{_configuration.RootDirectoryPath}/empty-{Guid.NewGuid()}.txt";
        _createdKeys.Add(key);

        await using (var stream = new AmazonS3WriteStream(NullLogger<AmazonS3WriteStream>.Instance, _s3Client, _configuration.BucketName, key))
        {
            // Write nothing, just dispose
        }

        // Empty file with no data and no multipart should not throw.
        // The file may or may not exist on S3 depending on implementation;
        // we just verify no exception was thrown.
    }

    internal AmazonS3ConnectorConfiguration GetConfiguration()
    {
        var accessKey = Environment.GetEnvironmentVariable("S3_ACCESSKEY");
        var secretKey = Environment.GetEnvironmentVariable("S3_SECRETKEY");
        var region = Environment.GetEnvironmentVariable("S3_REGION");
        var bucketName = Environment.GetEnvironmentVariable("S3_BUCKETNAME");

        var maskedSecret = string.IsNullOrWhiteSpace(secretKey) ? string.Empty
            : $"{secretKey[0..3]}{new string('*', Math.Max(secretKey.Length - 3, 0))}";
        TestOutputHelper.WriteLine(
            "Using AccessKey: '{0}', SecretKey: '{1}', Region: '{2}', BucketName: '{3}'.",
            accessKey,
            maskedSecret,
            region,
            bucketName);
        Assert.NotNull(accessKey);
        Assert.NotNull(maskedSecret);
        Assert.NotNull(region);
        Assert.NotNull(bucketName);

        var testPrefix = $"xunit-prefix-{DateTime.Now.Ticks}";
        var dictionary = new Dictionary<string, object>()
        {
            { AmazonS3ConfigurationConstants.AccessKey, accessKey },
            { AmazonS3ConfigurationConstants.SecretKey, secretKey },
            { AmazonS3ConfigurationConstants.Region, region },
            { AmazonS3ConfigurationConstants.BucketName, bucketName },
            { AmazonS3ConfigurationConstants.DirectoryName, testPrefix },
        };

        return new AmazonS3ConnectorConfiguration(dictionary);
    }
}
