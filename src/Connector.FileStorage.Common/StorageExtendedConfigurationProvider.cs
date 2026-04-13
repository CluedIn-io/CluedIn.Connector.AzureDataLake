using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Providers.ExtendedConfiguration;

namespace CluedIn.Connector.FileStorage.Common;

internal class FileStorageExtendedConfigurationProvider : IExtendedConfigurationProvider
{
    internal const string DefaultSourceName = "DataLakeExtendedConfigurationProvider";
    internal const string ReducedFormatsSourceName = $"{DefaultSourceName}_ReducedFormats";
    internal const string CustomCronScheduleName = StorageConfigurationConstants.CustomCronScheduleName;
    private const int DefaultPageSize = 20;
    private static readonly Option[] _scheduleOptions = CronSchedules.SupportedCronScheduleNames
        .Select(schedule => new Option(schedule.Key, $"{schedule.Value.Name} ({schedule.Value.Description})"))
        .Append(new Option(CustomCronScheduleName, "Custom Cron"))
        .ToArray();

    private static readonly Option[] _allOutputFormatOptions = StorageConfigurationConstants.OutputFormats.AllSupportedFormats
        .Select(name => new Option(name.ToLowerInvariant(), name))
        .ToArray();
    private static readonly Option[] _reducedOutputFormatOptions = StorageConfigurationConstants.OutputFormats.ReducedSupportedFormats
        .Select(name => new Option(name.ToLowerInvariant(), name))
        .ToArray();

    public Task<CanHandleResponse> CanHandle(ExecutionContext context, ExtendedConfigurationRequest request)
    {
        return Task.FromResult(new CanHandleResponse
        {
            CanHandle = DefaultSourceName.Equals(request?.Source) || ReducedFormatsSourceName.Equals(request?.Source),
        });
    }

    public async Task<ResolveOptionByValueResponse> ResolveOptionByValue(ExecutionContext context, ResolveOptionByValueRequest request)
    {
        var found = request.Key switch
        {
            StorageConfigurationConstants.OutputFormat
                => HandleOutputFormat(request.CurrentValues, request).Data
                    .SingleOrDefault(item => item.Value.Equals(request.Value, StringComparison.OrdinalIgnoreCase)),
            StorageConfigurationConstants.Schedule => HandleSchedule().Data.SingleOrDefault(item => item.Value.Equals(request.Value, StringComparison.OrdinalIgnoreCase)),
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
            StorageConfigurationConstants.OutputFormat => HandleOutputFormat(request.CurrentValues, request),
            StorageConfigurationConstants.Schedule => HandleSchedule(),
            _ => ResolveOptionsResponse.Empty,
        };
    }

    private static ResolveOptionsResponse HandleOutputFormat(IDictionary<string, string> currentValues, ExtendedConfigurationRequest request)
    {
        var options = ReducedFormatsSourceName.Equals(request?.Source)
            ? _reducedOutputFormatOptions
            : _allOutputFormatOptions;

        return new ResolveOptionsResponse
        {
            Data = options,
            Total = options.Length,
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
