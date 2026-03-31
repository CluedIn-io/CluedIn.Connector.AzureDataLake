using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3ExportEntitiesJob : StorageExportEntitiesJobBase
{
    public AmazonS3ExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IAmazonS3ConfigurationConstants dataLakeConstants,
        AmazonS3Factory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override string StreamIdDefaultStringFormat => "D";
}
