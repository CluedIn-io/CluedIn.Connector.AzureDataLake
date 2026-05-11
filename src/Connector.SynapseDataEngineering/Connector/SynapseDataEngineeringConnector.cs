using System;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

public class SynapseDataEngineeringConnector : DataLakeConnector
{
    public SynapseDataEngineeringConnector(
        ILogger<SynapseDataEngineeringConnector> logger,
        ApplicationContext applicationContext,
        SynapseDataEngineeringClient client,
        ISynapseDataEngineeringConstants constants,
        SynapseDataEngineeringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, applicationContext, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(SynapseDataEngineeringExportEntitiesJob);
}
