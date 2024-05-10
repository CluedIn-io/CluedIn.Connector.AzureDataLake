using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Connectors;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    internal class ExportEntitiesScheduleCheckerJob : AzureDataLakeJobBase
    {

        public static readonly Dictionary<string, string> CronSchedules = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [AzureDataLakeConstants.JobScheduleNames.Hourly] = "0 0/1 * * *",
            [AzureDataLakeConstants.JobScheduleNames.Daily] = "0 0 1-31 * *",
            [AzureDataLakeConstants.JobScheduleNames.Weekly] = "0 0 1-31 * 1",
            [AzureDataLakeConstants.JobScheduleNames.Never] = "0 5 31 2 *",
        };

        public ExportEntitiesScheduleCheckerJob(ApplicationContext appContext) : base(appContext)
        {
        }

        protected override async Task DoRunAsync(ExecutionContext context, JobArgs args)
        {
            var streamRepository = context.ApplicationContext.Container.Resolve<IStreamRepository>();
            var jobServerClient = context.ApplicationContext.Container.Resolve<IJobServerClient>();

            var streams = streamRepository.GetAllStreams().ToList();

            var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();

            foreach (var orgId in organizationIds)
            {
                var org = new Organization(context.ApplicationContext, orgId);

                foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                             x.ProviderId == new AzureDataLakeConstants(context.ApplicationContext).ProviderId))
                {
                    foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                    {
                        var executionContext = context.ApplicationContext.CreateExecutionContext(orgId);

                        var providerDefinitionId = stream.ConnectorProviderDefinitionId!.Value;
                        var containerName = stream.ContainerName;

                        var configurations = await AzureDataLakeConnectorJobData.Create(context,providerDefinitionId, containerName);

                        var cronSchedule = GetCronSchedule(executionContext, stream, configurations);

                        context.ApplicationContext.Container
                            .Resolve<ExportEntitiesJob>()
                            .Schedule(jobServerClient, cronSchedule, stream.Id.ToString());
                    }
                }
            }
        }

        private static string GetCronSchedule(ExecutionContext context, StreamModel stream, AzureDataLakeConnectorJobData configurations)
        {
            if (configurations.IsStreamCacheEnabled
                && stream.Status == StreamStatus.Started
                && CronSchedules.TryGetValue(configurations.Schedule, out var retrievedSchedule))
            {
                context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
                return retrievedSchedule;
            }

            context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

            return CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
        }
    }
}
