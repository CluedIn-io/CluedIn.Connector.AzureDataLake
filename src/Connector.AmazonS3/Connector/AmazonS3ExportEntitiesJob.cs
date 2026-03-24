using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3ExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public AmazonS3ExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        AmazonS3Client storageClient,
        IAmazonS3Constants dataLakeConstants,
        AmazonS3JobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, storageClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override string StreamIdDefaultStringFormat => "D";
}
