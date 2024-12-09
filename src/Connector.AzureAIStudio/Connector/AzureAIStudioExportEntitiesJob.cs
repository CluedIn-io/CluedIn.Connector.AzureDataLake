using System;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureAIStudio.Connector;

internal class AzureAIStudioExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AzureAIStudioExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AzureAIStudioClient dataLakeClient,
        IAzureAIStudioConstants dataLakeConstants,
        AzureAIStudioJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }
}
