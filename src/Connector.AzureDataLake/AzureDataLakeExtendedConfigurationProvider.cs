using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Providers.ExtendedConfiguration;
using CluedIn.Integration.PrivateServices.Configuration;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeExtendedConfigurationProvider : IExtendedConfigurationProvider
{
    private const int DefaultPageSize = 20;
    internal const string DefaultSourceName = "AzureDataLakeExtendedConfigurationProvider";
    internal const string AuthenticationSchemeSourceName = $"{DefaultSourceName}_AuthenticationScheme";

    private static readonly Option[] _allAuthenticationMethodsOptions = Enum.GetValues<AuthenticationMethods>()
        .Select(name => new Option(Value: name.ToString(), Label: typeof(AuthenticationMethods).GetMember(name.ToString()).First().GetCustomAttribute<DisplayAttribute>().Name))
        .ToArray();
    private static readonly Option[] _reducedAuthenticationMethodsOptions = Enum.GetValues<AuthenticationMethods>()
        .Except(new[] { AuthenticationMethods.WorkloadIdentity })
        .Select(name => new Option(Value: name.ToString(), Label: typeof(AuthenticationMethods).GetMember(name.ToString()).First().GetCustomAttribute<DisplayAttribute>().Name))
        .ToArray();

    private readonly bool _shouldEnableWorkloadIdentity;

    public AzureDataLakeExtendedConfigurationProvider(IAzureDataLakeConfigurationConstants constants)
    {
        _shouldEnableWorkloadIdentity = ConfigurationManagerEx.AppSettings.GetValue(
            constants.WorkloadIdentityAuthenticationMethodEnabledKeyName,
            constants.WorkloadIdentityAuthenticationMethodEnabledDefaultValue);
    }

    public Task<CanHandleResponse> CanHandle(ExecutionContext context, ExtendedConfigurationRequest request)
    {
        return Task.FromResult(new CanHandleResponse
        {
            CanHandle = DefaultSourceName.Equals(request?.Source) || AuthenticationSchemeSourceName.Equals(request?.Source),
        });
    }

    public async Task<ResolveOptionByValueResponse> ResolveOptionByValue(ExecutionContext context, ResolveOptionByValueRequest request)
    {
        var found = request.Key switch
        {
            AzureDataLakeConfigurationConstants.AuthenticationMethod => ResolveAuthenticationMethod(context, request),
            _ => null,
        };

        return new ResolveOptionByValueResponse
        {
            Option = found,
        };

        Option ResolveAuthenticationMethod(ExecutionContext context, ResolveOptionByValueRequest request)
        {
            var methods = HandleAuthenticationMethod(context, request.CurrentValues, request).Data;
            var value = request.Value;
            return methods.SingleOrDefault(item => item.Value.Equals(value, StringComparison.OrdinalIgnoreCase));
        }
    }

    public async Task<ResolveOptionsResponse> ResolveOptions(ExecutionContext context, ResolveOptionsRequest request)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(request);

        return request.Key switch
        {
            AzureDataLakeConfigurationConstants.AuthenticationMethod => HandleAuthenticationMethod(context, request.CurrentValues, request),
            _ => ResolveOptionsResponse.Empty,
        };
    }

    private ResolveOptionsResponse HandleAuthenticationMethod(ExecutionContext context, IDictionary<string, string> currentValues, ExtendedConfigurationRequest request)
    {
        var isSaaSDeployment = context.ApplicationContext.System.Configuration.GetIsSaaSDeployment();
        var shouldShowWorkloadIdentity = !isSaaSDeployment && _shouldEnableWorkloadIdentity;
        var options = shouldShowWorkloadIdentity
            ? _allAuthenticationMethodsOptions
            : _reducedAuthenticationMethodsOptions;
        return new ResolveOptionsResponse
        {
            Data = options,
            Total = options.Length,
            Page = 0,
            Take = DefaultPageSize,
        };
    }
}
