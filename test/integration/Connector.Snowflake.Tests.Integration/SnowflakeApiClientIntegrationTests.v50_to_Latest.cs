#if CLUEDIN_V50_OR_GREATER
using System.Threading.Tasks;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

public partial class SnowflakeApiClientIntegrationTests : IAsyncLifetime
{
    public ValueTask InitializeAsync()
    {
        if (SnowflakeTestCredentials.IsAvailable)
        {
            InitializeClient();
        }

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeClientAsync();
    }
}
#endif
