using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OutputDirectoryHelper
{
    public static Task<string> GetSubDirectory(ExecutionContext executionContext, IStorageConfiguration configuration, Guid streamId, string containerName, DateTimeOffset dataTime, string outputFormat)
    {
        if (configuration is OpenMirroringConnectorConfiguration casted &&
            !string.IsNullOrWhiteSpace(casted.TableName))
        {
            return PatternHelper.ReplaceNameUsingPatternAsync(
                executionContext,
                casted.TableName,
                streamId,
                containerName,
                dataTime,
                outputFormat);
        }

        return Task.FromResult(streamId.ToString("N"));
    }
}
