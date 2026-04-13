using System;

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
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, dataLakeStorageFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureAIStudioExportEntitiesJob);
}
