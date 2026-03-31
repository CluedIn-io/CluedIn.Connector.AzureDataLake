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
        AzureDataLakeFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
    }

    protected override string StreamIdDefaultStringFormat => "D";
}
