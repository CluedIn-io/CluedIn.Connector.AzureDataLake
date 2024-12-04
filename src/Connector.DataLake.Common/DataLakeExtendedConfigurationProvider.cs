using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Providers.ExtendedConfiguration;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeExtendedConfigurationProvider : IExtendedConfigurationProvider
{
    internal const string SourceName = "DataLakeExtendedConfigurationProvider";
    internal const string CustomCronScheduleName = DataLakeConstants.CustomCronScheduleName;
    private const int DefaultPageSize = 20;
    private static readonly Option[] _scheduleOptions = CronSchedules.SupportedCronScheduleNames
        .Select(schedule => new Option(schedule.Key, $"{schedule.Value.Name} ({schedule.Value.Description})"))
        .Append(new Option(CustomCronScheduleName, "Custom Cron"))
        .ToArray();

    private static readonly Option[] _outputFormatOptions = DataLakeConstants.OutputFormats.SupportedFormats
        .Select(name => new Option(name.ToLowerInvariant(), name))
        .ToArray();

    public Task<CanHandleResponse> CanHandle(ExecutionContext context, ExtendedConfigurationRequest request)
    {
        return Task.FromResult(new CanHandleResponse
        {
            CanHandle = request?.Source == SourceName
        });
    }

    public async Task<ResolveOptionByValueResponse> ResolveOptionByValue(ExecutionContext context, ResolveOptionByValueRequest request)
    {
        var found = request.Key switch
        {
            DataLakeConstants.OutputFormat => HandleOutputFormat(request.CurrentValues).Data.SingleOrDefault(item => item.Value.Equals(request.Value, StringComparison.OrdinalIgnoreCase)),
            DataLakeConstants.Schedule => HandleSchedule().Data.SingleOrDefault(item => item.Value.Equals(request.Value, StringComparison.OrdinalIgnoreCase)),
            _ => null,
        };

        return new ResolveOptionByValueResponse
        {
            Option = found,
        };
    }

    public async Task<ResolveOptionsResponse> ResolveOptions(ExecutionContext context, ResolveOptionsRequest request)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(request);

        return request.Key switch
        {
            DataLakeConstants.OutputFormat => HandleOutputFormat(request.CurrentValues),
            DataLakeConstants.Schedule => HandleSchedule(),
            _ => ResolveOptionsResponse.Empty,
        };
    }

    private static ResolveOptionsResponse HandleOutputFormat(IDictionary<string, string> currentValues)
    {
        return new ResolveOptionsResponse
        {
            Data = _outputFormatOptions,
            Total = _outputFormatOptions.Length,
            Page = 0,
            Take = DefaultPageSize,
        };
    }

    private static ResolveOptionsResponse HandleSchedule()
    {
        return new ResolveOptionsResponse
        {
            Data = _scheduleOptions,
            Total = _scheduleOptions.Length,
            Page = 0,
            Take = DefaultPageSize,
        };
    }
}
