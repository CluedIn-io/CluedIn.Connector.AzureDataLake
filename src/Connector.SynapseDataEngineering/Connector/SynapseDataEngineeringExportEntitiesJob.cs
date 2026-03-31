using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

internal class SynapseDataEngineeringExportEntitiesJob : StorageExportEntitiesJobBase
{
    public SynapseDataEngineeringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        ISynapseDataEngineeringConfigurationConstants dataLakeConstants,
        SynapseDataEngineeringFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }
}
