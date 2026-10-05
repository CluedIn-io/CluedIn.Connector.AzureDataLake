#if !CLUEDIN_V50_OR_GREATER
using System.Threading.Tasks;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

public partial class SnowflakeApiClientIntegrationTests : IAsyncLifetime
{
    public Task InitializeAsync()
    {
        if (SnowflakeTestCredentials.IsAvailable)
        {
            InitializeClient();
        }

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        return DisposeClientAsync();
    }
}
#endif
