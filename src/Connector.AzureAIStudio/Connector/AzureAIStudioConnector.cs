using System;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureAIStudio.Connector;

public class AzureAIStudioConnector : DataLakeConnector
{
    public AzureAIStudioConnector(
        ILogger<AzureAIStudioConnector> logger,
        ApplicationContext applicationContext,
        AzureAIStudioClient client,
        IAzureAIStudioConstants constants,
        AzureAIStudioJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureAIStudioExportEntitiesJob);
}
