using System;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDatabricks.Connector;

public class AzureDatabricksConnector : StorageConnectorBase
{
    public AzureDatabricksConnector(
        ILogger<AzureDatabricksConnector> logger,
        ApplicationContext applicationContext,
        IAzureDatabricksConfigurationConstants configurationConstants,
        AzureDatabricksStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(AzureDatabricksExportEntitiesJob);
}
