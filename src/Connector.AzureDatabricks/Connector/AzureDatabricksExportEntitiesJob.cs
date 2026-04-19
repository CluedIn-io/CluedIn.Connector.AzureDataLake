using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureDatabricks.Connector;

internal class AzureDatabricksExportEntitiesJob : StorageExportEntitiesJobBase
{
    public AzureDatabricksExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IAzureDatabricksConfigurationConstants configurationConstants,
        AzureDatabricksStorageFactory storageStorageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageStorageFactory, dateTimeOffsetProvider)
    {
    }
}
