using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core.Connectors;
using System;

namespace CluedIn.Connector.S3
{
    public class S3ConnectorProvider : ConnectorProviderBase
    {
        public S3ConnectorProvider(S3.Connector.S3Connector connector)
            : base(S3Constants.ProviderId, connector)
        {
        }
    }
}
