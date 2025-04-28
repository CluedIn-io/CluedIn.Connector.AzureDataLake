using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeConnector : DataLakeConnector
{
    public OneLakeConnector(
        ILogger<OneLakeConnector> logger,
        OneLakeClient client,
        IOneLakeConstants constants,
        OneLakeJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override async Task<ConnectionVerificationResult> VerifyConnectionInternal(ExecutionContext executionContext, IDataLakeJobData jobData)
    {
        var result = await base.VerifyConnectionInternal(executionContext, jobData);

        if (result?.Success != true || !jobData.IsStreamCacheEnabled)
        {
            return result;
        }

        var casted = (OneLakeConnectorJobData)jobData;
        if (!casted.ShouldLoadToTable)
        {
            return result;
        }

        if (!DataLakeConstants.OutputFormats.IsValid(casted.OutputFormat, isReducedSupportedFormat: true))
        {
            var supported = string.Join(',', DataLakeConstants.OutputFormats.ReducedSupportedFormats);
            var errorMessage = $"Format '{jobData.OutputFormat}' is not supported. Supported formats are {supported}.";
            return new ConnectionVerificationResult(false, errorMessage);
        }

        if (!casted.ShouldEscapeVocabularyKeys)
        {
            return new ConnectionVerificationResult(false, $"Must set {nameof(casted.ShouldEscapeVocabularyKeys)} when data should be loaded to table.");
        }

        return result;
    }
}
