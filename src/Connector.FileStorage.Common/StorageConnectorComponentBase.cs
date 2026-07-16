using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.EventHandlers;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Server;

using ComponentHost;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common;

public abstract class StorageConnectorComponentBase : ServiceApplicationComponent<IServer>
{
    private UpdateExportTargetEventHandler _updateExportTargetEventHandler;
    private ChangeStreamStateEventHandler _changeStreamStateEventHandler;
    private UpdateStreamEventHandler _updateStreamEventHandler;
    private RemoveStreamEventHandler _removeStreamEventHandler;

    protected StorageConnectorComponentBase(ComponentInfo componentInfo) : base(componentInfo)
    {
    }

    protected abstract string ConnectorComponentName { get; }
    protected abstract string ShortConnectorComponentName { get; }
    protected Type ExportEntitiesJobType { get; set; }

    protected virtual void DefaultStartInternal<TConfigurationConstants, TClientFactory, TExportJob>()
        where TConfigurationConstants : IStorageConfigurationConstants
        where TClientFactory : IStorageFactory
        where TExportJob : IStorageJob
    {
         if (ConfigurationManagerEx.AppSettings.GetFlag("Streams.Processing.Enabled", true))
         {
            ExportEntitiesJobType = typeof(TExportJob);

            var dataLakeConstants = Container.Resolve<TConfigurationConstants>();
            var jobDataFactory = Container.Resolve<TClientFactory>();
            var dateTimeOffsetProvider = Container.Resolve<IDateTimeOffsetProvider>();

            var migrator = GetDataMigrator(dataLakeConstants, jobDataFactory);
            _ = Task.Run(migrator.MigrateAsync);

            var scheduler = GetScheduler(dataLakeConstants, jobDataFactory, dateTimeOffsetProvider);

            _ = Task.Run(scheduler.RunAsync);

            SubscribeToEvents(dataLakeConstants, jobDataFactory, scheduler);
        }
        else
        {
            Log.LogInformation($"{ConnectorComponentName} scheduled jobs disabled");
        }

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

    private protected virtual void SubscribeToEvents(IStorageConfigurationConstants constants, IStorageFactory jobDataFactory, IScheduledJobQueue jobQueue)
    {
        var dateTimeProvider = Container.Resolve<IDateTimeOffsetProvider>();
        _updateExportTargetEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _changeStreamStateEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _updateStreamEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _removeStreamEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
    }

    private protected virtual IDataMigrator GetDataMigrator(IStorageConfigurationConstants constants, IStorageFactory jobDataFactory)
    {
        return new StorageDataMigrator(Log, ApplicationContext, Container.Resolve<DbContextOptions<CluedInEntities>>(), ShortConnectorComponentName, constants, jobDataFactory);
    }

    private protected virtual IScheduler GetScheduler(IStorageConfigurationConstants constants, IStorageFactory jobDataFactory, IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        return new StorageScheduler(Log, ShortConnectorComponentName, ApplicationContext, dateTimeOffsetProvider, constants, jobDataFactory, ExportEntitiesJobType);
    }
}
