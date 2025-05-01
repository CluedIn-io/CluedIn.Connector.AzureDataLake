using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.EventHandlers;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Server;

using ComponentHost;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeConnectorComponentBase : ServiceApplicationComponent<IServer>
{
    private UpdateExportTargetEventHandler _updateExportTargetEventHandler;
    private ChangeStreamStateEventHandler _changeStreamStateEventHandler;
    private UpdateStreamEventHandler _updateStreamEventHandler;
    private RemoveStreamEventHandler _removeStreamEventHandler;

    protected DataLakeConnectorComponentBase(ComponentInfo componentInfo) : base(componentInfo)
    {
    }

    protected abstract string ConnectorComponentName { get; }
    protected abstract string ShortConnectorComponentName { get; }
    protected Type ExportEntitiesJobType { get; set; }

    protected virtual void DefaultStartInternal<TDataLakeConstants, TDataLakeJobFactory, TDataLakeExportJob>()
        where TDataLakeConstants : IDataLakeConstants
        where TDataLakeJobFactory : IDataLakeJobDataFactory
        where TDataLakeExportJob : IDataLakeJob
    {
         if (ConfigurationManagerEx.AppSettings.GetFlag("Streams.Processing.Enabled", true))
         {
            ExportEntitiesJobType = typeof(TDataLakeExportJob);

            var dataLakeConstants = Container.Resolve<TDataLakeConstants>();
            var jobDataFactory = Container.Resolve<TDataLakeJobFactory>();
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

    private protected virtual void SubscribeToEvents(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory, IScheduledJobQueue jobQueue)
    {
        var dateTimeProvider = Container.Resolve<IDateTimeOffsetProvider>();
        _updateExportTargetEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _changeStreamStateEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _updateStreamEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
        _removeStreamEventHandler = new(ApplicationContext, constants, jobDataFactory, dateTimeProvider, ExportEntitiesJobType, jobQueue);
    }

    private protected virtual IDataMigrator GetDataMigrator(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory)
    {
        return new DataLakeDataMigrator(Log, ApplicationContext, Container.Resolve<DbContextOptions<CluedInEntities>>(), ShortConnectorComponentName, constants, jobDataFactory);
    }

    private protected virtual IScheduler GetScheduler(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory, IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        return new DataLakeScheduler(Log, ShortConnectorComponentName, ApplicationContext, dateTimeOffsetProvider, constants, jobDataFactory, ExportEntitiesJobType);
    }
}
