using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Jobs;
using CluedIn.Core.Server;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using ComponentHost;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake
{
    [Component(nameof(AzureDataLakeConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class AzureDataLakeConnectorComponent : ServiceApplicationComponent<IServer>
    {
        private UpdateExportTargetEventHandler _updateExportTargetHandler;
        private ChangeStreamStateEventHandler _changeStreamStateEvent;
        private UpdateStreamEventHandler _updateStreamEvent;
        static readonly NCrontab.CrontabSchedule _schedulerCron = NCrontab.CrontabSchedule.Parse("* * * * *");
        static readonly TimeSpan _initialSchedulerDelay = TimeSpan.FromSeconds(3);

        public AzureDataLakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
            Container.Install(new InstallComponents());
        }

        /// <summary>Starts this instance.</summary>
        public override void Start()
        {
            var providerId = AzureDataLakeConstants.DataLakeProviderId;
            #region Set existing streams to EventMode
            Task.Run(async () =>
            {
                try
                {
                    var upgradeSettingKey = "ADl Mode Migration";

                    var dbContext = new CluedInEntities(Container.Resolve<DbContextOptions<CluedInEntities>>());
                    var modeMigrationSetting = dbContext.Settings.FirstOrDefault(s =>
                        s.OrganizationId == Guid.Empty && s.Key == upgradeSettingKey);
                    if (modeMigrationSetting != null)
                    {
                        return;
                    }

                    var startedAt = DateTime.Now;

                    IStreamRepository streamRepository = null;
                    while (streamRepository == null)
                    {
                        if (DateTime.Now.Subtract(startedAt).TotalMinutes > 10)
                        {
                            Log.LogWarning($"Timeout resolving {nameof(IStreamRepository)}");
                            return;
                        }

                        try
                        {
                            streamRepository = Container.Resolve<IStreamRepository>();
                        }
                        catch
                        {
                            await Task.Delay(1000);
                        }
                    }

                    var streams = streamRepository.GetAllStreams().ToList();

                    var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();

                    foreach (var orgId in organizationIds)
                    {
                        var org = new Organization(ApplicationContext, orgId);

                        foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                                     x.ProviderId == providerId))
                        {
                            foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                            {
                                if (stream.Mode != StreamMode.EventStream)
                                {
                                    var executionContext = ApplicationContext.CreateExecutionContext(orgId);

                                    var model = new SetupConnectorModel
                                    {
                                        ConnectorProviderDefinitionId = provider.Id,
                                        Mode = StreamMode.EventStream,
                                        ContainerName = stream.ContainerName,
                                        DataTypes =
                                            (await streamRepository.GetStreamMappings(stream.Id))
                                            .Select(x => new DataTypeEntry
                                            {
                                                Key = x.SourceDataType,
                                                Type = x.SourceObjectType
                                            }).ToList(),
                                        ExistingContainerAction = ExistingContainerActionEnum.Archive,
                                        ExportIncomingEdges = stream.ExportIncomingEdges,
                                        ExportOutgoingEdges = stream.ExportOutgoingEdges,
                                        OldContainerName = stream.ContainerName,
                                    };

                                    Log.LogInformation($"Setting {nameof(StreamMode.EventStream)} for stream '{{StreamName}}' ({{StreamId}})", stream.Name, stream.Id);

                                    await streamRepository.SetupConnector(stream.Id, model, executionContext);
                                }
                            }
                        }
                    }

                    dbContext.Settings.Add(new Setting
                    {
                        Id = Guid.NewGuid(),
                        OrganizationId = Guid.Empty,
                        UserId = Guid.Empty,
                        Key = upgradeSettingKey,
                        Data = "Complete",
                    });
                    await dbContext.SaveChangesAsync();
                }
                catch (Exception ex)
                {
                    Log.LogError(ex, $"{ComponentName}: Upgrade error");
                }
            });
            #endregion

            _updateExportTargetHandler = new UpdateExportTargetEventHandler(ApplicationContext, providerId);
            _changeStreamStateEvent = new ChangeStreamStateEventHandler(ApplicationContext, providerId);
            _updateStreamEvent = new UpdateStreamEventHandler(ApplicationContext, providerId);
            _ = Task.Run(RunScheduler);

            Log.LogInformation($"{ComponentName} Registered");
            State = ServiceState.Started;
        }

        /// <summary>Stops this instance.</summary>
        public override void Stop()
        {
            if (State == ServiceState.Stopped)
                return;

            State = ServiceState.Stopped;
        }

        private record JobData(Type Type, Guid OrganizationId, Guid StreamId, string CronSchedule, DateTime NextRunTime);
        private async Task RunScheduler()
        {
            Log.LogDebug("Waiting for {InitialDelay} before starting scheduler.", _initialSchedulerDelay);
            await Task.Delay(_initialSchedulerDelay);

            var next = DateTime.UtcNow;

            var jobMap = new ConcurrentDictionary<Guid, JobData>();
            var providerId = AzureDataLakeConstants.DataLakeProviderId;
            while (true)
            {
                Log.LogDebug($"Scheduler begin scheduling.");

                try
                {
                    await Schedule(next, jobMap, providerId);
                }
                catch(Exception ex)
                {
                    Log.LogError(ex, "Error occurred when scheduling.");
                }

                next = _schedulerCron.GetNextOccurrence(DateTime.UtcNow.AddSeconds(1));
                Log.LogDebug("Scheduler completed scheduling. Next run at {NextRunTime}.", next);
                var delayToNextSchedule = next - DateTime.UtcNow;
                if (delayToNextSchedule.TotalSeconds > 0)
                {
                    await Task.Delay(delayToNextSchedule);
                }
            }
        }

        private async Task Schedule(DateTime next, ConcurrentDictionary<Guid, JobData> jobMap, Guid providerId)
        {
            var streamRepository = Container.Resolve<IStreamRepository>();
            var streams = streamRepository.GetAllStreams().ToList();

            var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();

            foreach (var orgId in organizationIds)
            {
                var org = new Organization(ApplicationContext, orgId);
                var executionContext = ApplicationContext.CreateExecutionContext(orgId);

                foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                             x.ProviderId == providerId))
                {
                    foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                    {
                        if (stream.Mode != StreamMode.Sync)
                        {
                            Log.LogDebug("Stream is not in {Mode}. Skipping", StreamMode.Sync);
                            continue;
                        }

                        var jobCronSchedule = await GetCronSchedule(executionContext, stream);
                        if (jobCronSchedule == AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never])
                        {
                            Log.LogDebug("Stream export for stream {StreamId} is disabled.", stream.Id);
                            _ = jobMap.TryRemove(stream.Id, out _);
                            continue;
                        }

                        var jobSchedule = NCrontab.CrontabSchedule.Parse(jobCronSchedule);
                        var nextJobTime = jobSchedule.GetNextOccurrence(next.AddSeconds(1));
                        var job = new JobData(typeof(AzureDataLakeExportEntitiesJob), stream.OrganizationId, stream.Id, jobCronSchedule, nextJobTime);
                        jobMap.AddOrUpdate(stream.Id, job, (_, existingJob) =>
                        {
                            if (existingJob.CronSchedule == job.CronSchedule)
                            {
                                Log.LogDebug("Stream {StreamId} is has same cron schedule, not updating next run time", stream.Id);
                                return existingJob;
                            }

                            Log.LogDebug("Stream {StreamId} is has different cron schedule, updating next run time", stream.Id);
                            return job;
                        });
                    }
                }
            }

            Log.LogDebug("Scheduler found {TotalJobs} jobs.", jobMap.Count);

            var executedJobData = new List<JobData>();
            foreach (var jobDataKvp in jobMap)
            {
                var jobData = jobDataKvp.Value;
                if (jobData.NextRunTime > next)
                {
                    Log.LogDebug("Job '{JobType}' for Stream {StreamId} is not supposed to run now. Next run time at {JobNextRunTime} based on {CronSchedule} cron.",
                        jobData.Type,
                        jobData.StreamId,
                        jobData.NextRunTime,
                        jobData.CronSchedule);
                    continue;
                }

                _ = Task.Run(async () =>
                {
                    try
                    {
                        var jobInstance = Container.Resolve(jobData.Type) as AzureDataLakeJobBase;
                        if (jobInstance == null)
                        {
                            throw new ApplicationException($"Job {jobInstance.GetType()} is not of type {typeof(AzureDataLakeJobBase)}.");
                        }
                        var executionContext = ApplicationContext.CreateExecutionContext(jobData.OrganizationId);
                        await jobInstance.DoRunAsync(executionContext, new JobArgs
                        {
                            Message = jobData.StreamId.ToString(),
                            Schedule = jobData.CronSchedule,
                        });
                    }
                    catch (Exception ex)
                    {
                        Log.LogError(ex, "Error when executing job.");
                    }
                });
                executedJobData.Add(jobData);
            }

            foreach (var jobData in executedJobData)
            {
                var jobCronSchedule = NCrontab.CrontabSchedule.Parse(jobData.CronSchedule);
                var nextJobTime = jobCronSchedule.GetNextOccurrence(next.AddSeconds(1));

                var newJobData = jobData with { NextRunTime = nextJobTime };
                var result = jobMap.AddOrUpdate(newJobData.StreamId, newJobData, (_, _) => newJobData);

                if (result != newJobData)
                {
                    Log.LogWarning("Scheduler failed to update job next run time for stream {StreamId}.", jobData.StreamId);
                }
            }
        }

        private static async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream)
        {
            var containerName = stream.ContainerName;
            var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
            var configurations = await AzureDataLakeConnectorJobData.Create(context, providerDefinitionId, containerName);
            if (configurations.IsStreamCacheEnabled
                && stream.Status == StreamStatus.Started
                && AzureDataLakeConstants.CronSchedules.TryGetValue(configurations.Schedule, out var retrievedSchedule))
            {
                context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
                return retrievedSchedule;
            }

            context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

            return AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
        }

        public string ComponentName => "Azure Data Lake Storage Gen2";
    }
}
