using System;

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
        SynapseDataEngineeringFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(SynapseDataEngineeringExportEntitiesJob);
}
