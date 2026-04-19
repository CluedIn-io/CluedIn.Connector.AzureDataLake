using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Streams;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3ExportEntitiesJob : StorageExportEntitiesJobBase
{
    public AmazonS3ExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        IAmazonS3ConfigurationConstants configurationConstants,
        AmazonS3StorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
    }

    protected override string StreamIdDefaultStringFormat => "D";

    protected override string TransformMetadataKey(string key)
    {
        return key.ToLowerInvariant();
    }
}
