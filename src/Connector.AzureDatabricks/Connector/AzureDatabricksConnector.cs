﻿using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDatabricks.Connector;

public class AzureDatabricksConnector : DataLakeConnector
{
    public AzureDatabricksConnector(
        ILogger<AzureDatabricksConnector> logger,
        AzureDatabricksClient client,
        IAzureDatabricksConstants constants,
        AzureDatabricksJobDataFactory dataLakeJobDataFactory,
        ISystemClock systemClock)
        : base(logger, client, constants, dataLakeJobDataFactory, systemClock)
    {
    }
}
