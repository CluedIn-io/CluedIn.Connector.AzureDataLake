using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

internal class SynapseDataEngineeringExportEntitiesJob : StorageExportEntitiesJobBase
{
    public SynapseDataEngineeringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        ISynapseDataEngineeringConfigurationConstants configurationConstants,
        SynapseDataEngineeringStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
    }
}
