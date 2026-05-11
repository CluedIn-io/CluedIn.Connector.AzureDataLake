using System;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDatabricks.Connector;

public class AzureDatabricksConnector : DataLakeConnector
{
    public AzureDatabricksConnector(
        ILogger<AzureDatabricksConnector> logger,
        ApplicationContext applicationContext,
        AzureDatabricksClient client,
        IAzureDatabricksConstants constants,
        AzureDatabricksJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureDatabricksExportEntitiesJob);
}
