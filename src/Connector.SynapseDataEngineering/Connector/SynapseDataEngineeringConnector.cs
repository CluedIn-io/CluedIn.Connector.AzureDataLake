using System;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

public class SynapseDataEngineeringConnector : DataLakeConnector
{
    public SynapseDataEngineeringConnector(
        ILogger<SynapseDataEngineeringConnector> logger,
        SynapseDataEngineeringClient client,
        ISynapseDataEngineeringConstants constants,
        SynapseDataEngineeringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Type ExportJobType => typeof(SynapseDataEngineeringExportEntitiesJob);
}
