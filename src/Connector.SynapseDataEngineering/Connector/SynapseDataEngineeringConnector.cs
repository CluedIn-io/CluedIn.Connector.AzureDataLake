using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

public class SynapseDataEngineeringConnector : DataLakeConnector
{
    public SynapseDataEngineeringConnector(
        ILogger<SynapseDataEngineeringConnector> logger,
        SynapseDataEngineeringClient client,
        ISynapseDataEngineeringConstants constants,
        SynapseDataEngineeringJobDataFactory dataLakeJobDataFactory,
        ISystemClock systemClock)
        : base(logger, client, constants, dataLakeJobDataFactory, systemClock)
    {
    }
}
