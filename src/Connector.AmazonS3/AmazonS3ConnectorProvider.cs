using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

using System.Collections.Generic;

namespace CluedIn.Connector.AmazonS3;

public class AmazonS3ConnectorProvider : ConnectorProviderBase<AmazonS3ConnectorProvider>
{
    public AmazonS3ConnectorProvider([NotNull] ApplicationContext appContext,
        IAmazonS3ConfigurationConstants configuration, ILogger<AmazonS3ConnectorProvider> logger)
        : base(appContext, configuration, logger)
    {
    }

    protected override IEnumerable<string> ProviderNameParts => new[]
    {
        AmazonS3ConfigurationConstants.BucketName,
        AmazonS3ConfigurationConstants.Region,
        AmazonS3ConfigurationConstants.DirectoryName,
    };
}
