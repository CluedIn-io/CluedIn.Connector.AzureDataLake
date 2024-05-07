using CluedIn.Core;
using CluedIn.Core.Providers.ExtendedConfiguration;
using System;
using System.Threading.Tasks;
using static CluedIn.Connector.AzureDataLake.AzureDataLakeConstants;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeExtendedConfigurationProvider : IExtendedConfigurationProvider
{
    public async Task<ResolveOptionsResponse> ResolveOptions(ExecutionContext context, ResolveOptionsRequest request)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(request);

        if (request.Key == ExtendedConfigurationProperties.OutputFormat)
            return new ResolveOptionsResponse
            {
                Options = new[]
                {
                    new Option("json", "JSON"),
                    new Option("parquet", "PARQUET"),
                    //new Option("csv", "CSV"),
                }
            };

        return new ResolveOptionsResponse
        {
            Options = Array.Empty<Option>(),
        };
    }
}
