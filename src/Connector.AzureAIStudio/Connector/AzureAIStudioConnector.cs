using System;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureAIStudio.Connector;

public class AzureAIStudioConnector : StorageConnectorBase
{
    public AzureAIStudioConnector(
        ILogger<AzureAIStudioConnector> logger,
        ApplicationContext applicationContext,
        IAzureAIStudioConfigurationConstants constants,
        AzureAIStudioStorageFactory dataLakeStorageFactory,
        ITimeProvider timeProvider)
        : base(logger, applicationContext, constants, dataLakeStorageFactory, timeProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureAIStudioExportEntitiesJob);
}
