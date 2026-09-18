using System;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

public class SynapseDataEngineeringConnector : StorageConnectorBase
{
    public SynapseDataEngineeringConnector(
        ILogger<SynapseDataEngineeringConnector> logger,
        ApplicationContext applicationContext,
        ISynapseDataEngineeringConfigurationConstants constants,
        SynapseDataEngineeringStorageFactory dataLakeJobDataStorageFactory,
        ITimeProvider timeProvider)
        : base(logger, applicationContext, constants, dataLakeJobDataStorageFactory, timeProvider)
    {
    }

    protected override Type ExportJobType => typeof(SynapseDataEngineeringExportEntitiesJob);
}
