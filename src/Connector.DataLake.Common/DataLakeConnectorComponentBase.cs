using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.DataLake.Common.EventHandlers;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Server;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using ComponentHost;

using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeConnectorComponentBase : ServiceApplicationComponent<IServer>
{
    private const string MigrationLockConnectionString = "Locking";
    private UpdateExportTargetEventHandler _updateExportTargetHandler;
    private ChangeStreamStateEventHandler _changeStreamStateEvent;
    private UpdateStreamEventHandler _updateStreamEvent;

    private static readonly NCrontab.CrontabSchedule _schedulerCron = NCrontab.CrontabSchedule.Parse("* * * * *");
    private static readonly TimeSpan _initialSchedulerDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan _schedulerErrorDelay = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan _migrationRetryDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _migrationLockTimeout = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan _migrationRetryTimeout = TimeSpan.FromMinutes(10);
    private static readonly int _migrationMaxRetries = 3;

    protected DataLakeConnectorComponentBase(ComponentInfo componentInfo) : base(componentInfo)
    {
    }

    protected abstract string ConnectorComponentName { get; }

    protected virtual void DefaultStartInternal<TDataLakeConstants, TDataLakeJobFactory, TDataLakeExportJob>()
        where TDataLakeConstants : IDataLakeConstants
        where TDataLakeJobFactory : IDataLakeJobDataFactory
        where TDataLakeExportJob : IDataLakeJob
    {
        var dataLakeConstants = Container.Resolve<TDataLakeConstants>();
        var jobDataFactory = Container.Resolve<TDataLakeJobFactory>();
        _ = Task.Run(() => RunMigrationsWithLock(dataLakeConstants));
        var exportEntitiesJobType = typeof(TDataLakeExportJob);
        SubscribeToEvents(dataLakeConstants, exportEntitiesJobType);
        _ = Task.Run(() => RunScheduler(dataLakeConstants, jobDataFactory, exportEntitiesJobType));

        Log.LogInformation($"{ConnectorComponentName} Registered");
        State = ServiceState.Started;
    }

    /// <summary>Stops this instance.</summary>
    public override void Stop()
    {
        if (State == ServiceState.Stopped)
        {
            return;
        }

        State = ServiceState.Stopped;
    }

    protected void SubscribeToEvents(IDataLakeConstants constants, Type exportEntitiesJobType)
    {
        _updateExportTargetHandler = new(ApplicationContext, constants, exportEntitiesJobType);
        _changeStreamStateEvent = new(ApplicationContext, constants, exportEntitiesJobType);
        _updateStreamEvent = new(ApplicationContext, constants, exportEntitiesJobType);
    }

    protected async Task MigrateAccountId(IDataLakeConstants constants)
    {
        await MigrateForOrganizations("EmptyAccountId", constants, migrateAccountIdForProviderDefinition);
        async Task migrateAccountIdForProviderDefinition(ExecutionContext context, string componentMigrationName, IDataLakeConstants constants)
        {
            var organizationId = context.Organization.Id;
            var store = context.Organization.DataStores.GetDataStore<ProviderDefinition>();
            var definitions = await store.SelectAsync(
                context,
                definition => definition.OrganizationId == context.Organization.Id
                                && definition.ProviderId == constants.ProviderId);
            foreach (var definition in definitions)
            {
                if (definition.AccountId != string.Empty)
                {
                    Log.LogDebug("Skipping provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                        componentMigrationName,
                        organizationId,
                        definition.Id);
                    continue;
                }

                Log.LogInformation("Begin provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
                definition.AccountId = AccountIdHelper.Generate(constants.ProviderId, definition.Id);
                await store.UpdateAsync(context, definition);
                Log.LogInformation("End provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
            }
        }
    }

    protected virtual async Task RunMigrationsWithLock(IDataLakeConstants constants)
    {
        var migrationErrorCount = 0;

        var migrationStart = DateTimeOffset.UtcNow;
        var shouldRetry = true;

        if (!ApplicationContext.System.ConnectionStrings.ConnectionStringExists(MigrationLockConnectionString))
        {
            throw new InvalidOperationException($"Connection string {MigrationLockConnectionString} is not found.");
        }

        var connectionString = ApplicationContext.System.ConnectionStrings.GetConnectionString(MigrationLockConnectionString);
        while (true)
        {
            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                var transaction = connection.BeginTransaction();
                Log.LogDebug("Try to acquire lock for '{ConnectorComponentName}' migration.", ConnectorComponentName);
                var hasAcquiredLock = await DistributedLockHelper.TryAcquireExclusiveLock(
                    transaction,
                    $"{ConnectorComponentName}-Migration",
                    (int)_migrationLockTimeout.TotalMilliseconds);
                if (hasAcquiredLock)
                {
                    Log.LogDebug("Acquired lock for '{ConnectorComponentName}' migration.", ConnectorComponentName);
                    await tryRunMigrations();
                    if (!shouldRetry)
                    {
                        return;
                    }
                }
                else
                {
                    Log.LogDebug("Failed to acquire lock for '{ConnectorComponentName}' migration.", ConnectorComponentName);
                }
            }
            catch (Exception ex)
            {
                Log.LogWarning(ex, "Error occurred while trying to acquire lock for '{ConnectorComponentName}' migration.", ConnectorComponentName);
            }

            var elapsed = DateTimeOffset.UtcNow - migrationStart;
            if (elapsed > _migrationRetryTimeout)
            {
                Log.LogError("Timeout while trying to perform '{ConnectorComponentName}' migration.", ConnectorComponentName);
                return;
            }

            Log.LogDebug("Retry '{ConnectorComponentName}' migration in {DelayBetweenMigrationRetries}.", ConnectorComponentName, _migrationRetryDelay);
            await Task.Delay(_migrationRetryDelay);
        }

        async Task tryRunMigrations()
        {
            try
            {
                Log.LogDebug("Begin '{ConnectorComponentName}' migration.", ConnectorComponentName);
                await RunMigrations(constants);
                Log.LogDebug("End '{ConnectorComponentName}' migration.", ConnectorComponentName);
                shouldRetry = false;
            }
            catch (Exception ex)
            {
                migrationErrorCount++;

                if (migrationErrorCount > _migrationMaxRetries)
                {
                    Log.LogWarning(ex, "Failed to perform '{ConnectorComponentName}' migration after {TotalAttempts} attempts. Giving up.", ConnectorComponentName, migrationErrorCount);
                    shouldRetry = false;
                }

                Log.LogWarning(ex, "Error occurred while trying to perform '{ConnectorComponentName}' migration.", ConnectorComponentName);
            }
        }
    }

    protected virtual async Task RunMigrations(IDataLakeConstants constants)
    {
        await MigrateAccountId(constants);
    }

    protected async Task Migrate(
        string migrationName,
        IDataLakeConstants constants,
        Func<string, IDataLakeConstants, Task> migrateTask)
    {
        var componentMigrationName = $"{ConnectorComponentName}:{migrationName}";
        try
        {
            var dbContext = new CluedInEntities(Container.Resolve<DbContextOptions<CluedInEntities>>());
            var migrationSetting = dbContext.Settings
                    .FirstOrDefault(setting => setting.OrganizationId == Guid.Empty
                                    && setting.Key == componentMigrationName);

            if (migrationSetting != null)
            {
                return;
            }

            Log.LogInformation("Begin migration: '{MigrationName}'.", componentMigrationName);

            await migrateTask(componentMigrationName, constants);

            dbContext.Settings.Add(new Setting
            {
                Id = Guid.NewGuid(),
                OrganizationId = Guid.Empty,
                UserId = Guid.Empty,
                Key = componentMigrationName,
                Data = "Complete",
            });
            await dbContext.SaveChangesAsync();
            Log.LogInformation("End migration: '{MigrationName}'.", componentMigrationName);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Error trying to perform migration: '{MigrationName}'.", componentMigrationName);
        }
    }

    protected async Task MigrateForOrganizations(
        string migrationName,
        IDataLakeConstants constants,
        Func<ExecutionContext, string, IDataLakeConstants, Task> migrateTask)
    {
        await Migrate(migrationName, constants, migrateProviderDefinition);
        async Task migrateProviderDefinition(string componentMigrationName, IDataLakeConstants constants)
        {
            var orgDataStore = ApplicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
            var organizationProfiles = await orgDataStore.SelectAsync(ApplicationContext.System.CreateExecutionContext(), _ => true);
            foreach (var organizationProfile in organizationProfiles)
            {
                var organizationId = organizationProfile.Id;
                Log.LogInformation("Begin organization migration: '{MigrationName}' for organization '{OrganizationId}'.", componentMigrationName, organizationId);
                var organization = new Organization(ApplicationContext, organizationId);
                var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
                await migrateTask(executionContext, componentMigrationName, constants);
                Log.LogInformation("End organization migration: '{MigrationName}' for organization '{OrganizationId}'.", componentMigrationName, organizationId);
            }
        }
    }

    protected async Task RunScheduler(IDataLakeConstants dataLakeConstants, IDataLakeJobDataFactory dataLakeJobDataFactory, Type jobType)
    {
        Log.LogDebug("Waiting for {InitialDelay} before starting scheduler.", _initialSchedulerDelay);
        await Task.Delay(_initialSchedulerDelay);

        var next = DateTime.UtcNow;

        var jobMap = new ConcurrentDictionary<Guid, JobData>();
        while (true)
        {
            Log.LogDebug($"Scheduler begin scheduling.");

            try
            {
                await Schedule(next, jobMap, dataLakeConstants, dataLakeJobDataFactory, jobType);
                next = _schedulerCron.GetNextOccurrence(DateTime.UtcNow.AddSeconds(1));
                Log.LogDebug("Scheduler completed scheduling. Next run at {NextRunTime}.", next);
                var delayToNextSchedule = next - DateTime.UtcNow;
                if (delayToNextSchedule.TotalSeconds > 0)
                {
                    await Task.Delay(delayToNextSchedule);
                }
            }
            catch(Exception ex)
            {
                Log.LogError(ex, "Error occurred when scheduling. Waiting {DelayAfterError} before next schedule run.", _schedulerErrorDelay);
                await Task.Delay(_schedulerErrorDelay);
            }
        }
    }

    protected async Task Schedule(DateTime next, ConcurrentDictionary<Guid, JobData> jobMap, IDataLakeConstants dataLakeConstants, IDataLakeJobDataFactory dataLakeJobDataFactory, Type jobType)
    {
        var streamRepository = Container.Resolve<IStreamRepository>();
        var streams = streamRepository.GetAllStreams().ToList();

        var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();

        foreach (var orgId in organizationIds)
        {
            var org = new Organization(ApplicationContext, orgId);
            var executionContext = ApplicationContext.CreateExecutionContext(orgId);

            foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                         x.ProviderId == dataLakeConstants.ProviderId))
            {
                foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                {
                    if (stream.Mode != StreamMode.Sync)
                    {
                        Log.LogDebug("Stream {StreamId} is not in {Mode} mode. Skipping", stream.Id, StreamMode.Sync);
                        continue;
                    }

                    var jobCronSchedule = await GetCronSchedule(executionContext, stream, dataLakeJobDataFactory);
                    if (jobCronSchedule == DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never])
                    {
                        Log.LogDebug("Stream export for stream {StreamId} is disabled.", stream.Id);
                        _ = jobMap.TryRemove(stream.Id, out _);
                        continue;
                    }

                    var jobSchedule = NCrontab.CrontabSchedule.Parse(jobCronSchedule);
                    var nextJobTime = jobSchedule.GetNextOccurrence(next.AddSeconds(1));
                    var job = new JobData(jobType, stream.OrganizationId, stream.Id, jobCronSchedule, nextJobTime);
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
                    var jobInstance = Container.Resolve(jobData.Type) as DataLakeJobBase;
                    if (jobInstance == null)
                    {
                        throw new ApplicationException($"Job {jobData.Type} is not of type {typeof(DataLakeJobBase)}.");
                    }
                    var executionContext = ApplicationContext.CreateExecutionContext(jobData.OrganizationId);
                    await jobInstance.DoRunAsync(executionContext, new DataLakeJobArgs
                    {
                        Message = jobData.StreamId.ToString(),
                        Schedule = jobData.CronSchedule,
                        IsTriggeredFromJobServer = false,
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

    private static async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream, IDataLakeJobDataFactory dataLakeJobDataFactory)
    {
        var containerName = stream.ContainerName;
        var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
        var configurations = await dataLakeJobDataFactory.GetConfiguration(context, providerDefinitionId, containerName);
        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && DataLakeConstants.CronSchedules.TryGetValue(configurations.Schedule, out var retrievedSchedule))
        {
            context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
            return retrievedSchedule;
        }

        context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

        return DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never];
    }

    protected record JobData(Type Type, Guid OrganizationId, Guid StreamId, string CronSchedule, DateTime NextRunTime);
}
