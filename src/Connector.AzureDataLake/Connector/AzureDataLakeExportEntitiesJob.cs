using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeExportEntitiesJob : StorageExportEntitiesJobBase
{
    public AzureDataLakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IAzureDataLakeConfigurationConstants configurationConstants,
        AzureDataLakeStorageFactory storageStorageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageStorageFactory, dateTimeOffsetProvider)
    {
    }

    protected override string StreamIdDefaultStringFormat => "D";
}
