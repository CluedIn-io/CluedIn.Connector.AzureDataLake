#if !CLUEDIN_V50
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;

using Xunit;

namespace CluedIn.Connector.AmazonS3.Tests.Integration;

public partial class AmazonS3WriteStreamTests : IAsyncLifetime
{
    public Task InitializeAsync()
    {
        _configuration = GetConfiguration();
        var region = RegionEndpoint.GetBySystemName(_configuration.Region);
        _s3Client = new AmazonS3Client(_configuration.AccessKey, _configuration.SecretKey, region);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        foreach (var key in _createdKeys)
        {
            try
            {
                await _s3Client.DeleteObjectAsync(_configuration.BucketName, key);
            }
            catch
            {
                // best-effort cleanup
            }
        }

        _s3Client?.Dispose();
    }
}
#endif
