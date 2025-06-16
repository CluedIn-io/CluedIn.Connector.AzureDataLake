using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDatabricks.Connector;

public class AzureDatabricksConnector : DataLakeConnector
{
    public AzureDatabricksConnector(
        ILogger<AzureDatabricksConnector> logger,
        AzureDatabricksClient client,
        IAzureDatabricksConstants constants,
        AzureDatabricksJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureDatabricksExportEntitiesJob);
}
