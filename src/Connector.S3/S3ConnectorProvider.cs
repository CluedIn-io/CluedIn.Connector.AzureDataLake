using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.S3
{
    public class S3ConnectorProvider : ConnectorProviderBase<S3ConnectorProvider>
    {
        public S3ConnectorProvider(ApplicationContext appContext, IConfigurationConstants configurationConstants, ILogger<S3ConnectorProvider> logger)
            : base(appContext, configurationConstants, logger)
        {
        }
    }
}
