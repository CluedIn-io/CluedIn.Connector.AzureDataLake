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
        AzureAIStudioJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureAIStudioExportEntitiesJob);
}
