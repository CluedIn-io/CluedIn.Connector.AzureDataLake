using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake;

public class SnowflakeConnectorProvider : ConnectorProviderBase<SnowflakeConnectorProvider>
{
    public SnowflakeConnectorProvider([NotNull] ApplicationContext appContext,
        ISnowflakeConfigurationConstants configuration, ILogger<SnowflakeConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        SnowflakeConfigurationConstants.Account,
        SnowflakeConfigurationConstants.Database,
        SnowflakeConfigurationConstants.Schema,
        SnowflakeConfigurationConstants.TableName,
    };
}
