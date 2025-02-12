using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;


namespace CluedIn.Connector.FabricMirroring.Connector;

public class FabricMirroringConnector : DataLakeConnector
{
    public FabricMirroringConnector(
        ILogger<FabricMirroringConnector> logger,
        FabricMirroringClient client,
        IFabricMirroringConstants constants,
        FabricMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override Task VerifyDataLakeConnection(IDataLakeJobData jobData)
    {
        //TODO: verify path exists only if mirroring created
        // find out a way to see if it's called by test connection or healthcheck (might have to do ugly reflection to look at stack)
        // probably could do it by verifying if jobdata from db = jobdata created from passed config
        // but we need to be able to get provider definition id, which is not passed (wtf!)
        return base.VerifyDataLakeConnection(jobData);
    }
}
