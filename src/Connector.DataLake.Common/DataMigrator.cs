using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;

using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

internal class DataMigrator : IDataMigrator
{
    private const string MigrationLockConnectionString = "Locking";
    private static readonly TimeSpan _migrationRetryDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _migrationLockTimeout = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan _migrationRetryTimeout = TimeSpan.FromMinutes(10);
    private static readonly int _migrationMaxRetries = 3;

    protected string _componentName;
    protected ILogger _logger;
    protected ApplicationContext _applicationContext;

    private DbContextOptions<CluedInEntities> _cluedInEntitiesDbContextOptions;

    public DataMigrator(
        ILogger logger,
        ApplicationContext applicationContext,
        DbContextOptions<CluedInEntities> cluedInEntitiesDbContextOptions,
        string componentName)
    {
        if (string.IsNullOrEmpty(componentName))
        {
            throw new ArgumentException($"'{nameof(componentName)}' cannot be null or empty.", nameof(componentName));
        }

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _cluedInEntitiesDbContextOptions = cluedInEntitiesDbContextOptions ?? throw new ArgumentNullException(nameof(cluedInEntitiesDbContextOptions));
        _componentName = componentName;
    }

    public async Task MigrateAsync()
    {
        await RunMigrationsWithLock();
    }

    protected virtual async Task RunMigrationsWithLock()
    {
        var migrationErrorCount = 0;
        var migrationStart = DateTimeOffset.UtcNow;
        var shouldRetry = true;

        if (!_applicationContext.System.ConnectionStrings.ConnectionStringExists(MigrationLockConnectionString))
        {
            throw new InvalidOperationException($"Connection string {MigrationLockConnectionString} is not found.");
        }

        var connectionString = _applicationContext.System.ConnectionStrings.GetConnectionString(MigrationLockConnectionString);
        while (true)
        {
            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                var transaction = connection.BeginTransaction();
                _logger.LogDebug("Try to acquire lock for '{ConnectorComponentName}' migration.", _componentName);
                var hasAcquiredLock = await DistributedLockHelper.TryAcquireExclusiveLock(
                    transaction,
                    $"{_componentName}-Migration",
                    (int)_migrationLockTimeout.TotalMilliseconds);
                if (hasAcquiredLock)
                {
                    _logger.LogDebug("Acquired lock for '{ConnectorComponentName}' migration.", _componentName);
                    await tryRunMigrations();
                    if (!shouldRetry)
                    {
                        return;
                    }
                }
                else
                {
                    _logger.LogDebug("Failed to acquire lock for '{ConnectorComponentName}' migration.", _componentName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error occurred while trying to acquire lock for '{ConnectorComponentName}' migration.", _componentName);
            }

            var elapsed = DateTimeOffset.UtcNow - migrationStart;
            if (elapsed > _migrationRetryTimeout)
            {
                _logger.LogError("Timeout while trying to perform '{ConnectorComponentName}' migration.", _componentName);
                return;
            }

            _logger.LogDebug("Retry '{ConnectorComponentName}' migration in {DelayBetweenMigrationRetries}.", _componentName, _migrationRetryDelay);
            await Task.Delay(_migrationRetryDelay);
        }

        async Task tryRunMigrations()
        {
            try
            {
                _logger.LogDebug("Begin '{ConnectorComponentName}' migration.", _componentName);
                await RunMigrations();
                _logger.LogDebug("End '{ConnectorComponentName}' migration.", _componentName);
                shouldRetry = false;
            }
            catch (Exception ex)
            {
                migrationErrorCount++;

                if (migrationErrorCount > _migrationMaxRetries)
                {
                    _logger.LogWarning(ex, "Failed to perform '{ConnectorComponentName}' migration after {TotalAttempts} attempts. Giving up.", _componentName, migrationErrorCount);
                    shouldRetry = false;
                }

                _logger.LogWarning(ex, "Error occurred while trying to perform '{ConnectorComponentName}' migration.", _componentName);
            }
        }
    }

    protected virtual Task RunMigrations()
    {
        return Task.CompletedTask;
    }

    protected async Task MigrateForOrganizations(
        string migrationName,
        Func<ExecutionContext, string, Task> migrateTask)
    {
        await Migrate(migrationName, migrateOrganizationTask);
        async Task migrateOrganizationTask(string migrationName)
        {
            var orgDataStore = _applicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
            var organizationProfiles = await orgDataStore.SelectAsync(_applicationContext.System.CreateExecutionContext(), _ => true);
            foreach (var organizationProfile in organizationProfiles)
            {
                var organizationId = organizationProfile.Id;
                _logger.LogInformation("Begin organization migration: '{MigrationName}' for organization '{OrganizationId}'.", migrationName, organizationId);
                var organization = new Organization(_applicationContext, organizationId);
                var executionContext = _applicationContext.CreateExecutionContext(organizationId);
                await migrateTask(executionContext, migrationName);
                _logger.LogInformation("End organization migration: '{MigrationName}' for organization '{OrganizationId}'.", migrationName, organizationId);
            }
        }
    }

    public async Task<bool> IsMigrationPerformedAsync(Guid organizationId, string key)
    {
        var dbContext = new CluedInEntities(_cluedInEntitiesDbContextOptions);
        var migrationSetting = await dbContext.Settings
                .FirstOrDefaultAsync(setting => setting.OrganizationId == Guid.Empty
                                && setting.Key == key);

        return migrationSetting != null;
    }

    public async Task SetMigrationPerformedAsync(Guid organizationId, string key)
    {
        var dbContext = new CluedInEntities(_cluedInEntitiesDbContextOptions);
        dbContext.Settings.Add(new Setting
        {
            Id = Guid.NewGuid(),
            OrganizationId = Guid.Empty,
            UserId = Guid.Empty,
            Key = key,
            Data = "Complete",
        });

        await dbContext.SaveChangesAsync();
    }

    protected async Task Migrate(
        string migrationName,
        Func<string, Task> migrateTask)
    {
        var componentMigrationName = $"{_componentName}:Migration:{migrationName}";
        try
        {
            var hasMigrationPerformed = await IsMigrationPerformedAsync(Guid.Empty, componentMigrationName);
            if (hasMigrationPerformed)
            {
                _logger.LogDebug("Skipping migration: '{MigrationName}' because it has already been performed.", componentMigrationName);
                return;
            }

            _logger.LogInformation("Begin migration: '{MigrationName}'.", componentMigrationName);

            await migrateTask(componentMigrationName);

            await SetMigrationPerformedAsync(Guid.Empty, componentMigrationName);
            _logger.LogInformation("End migration: '{MigrationName}'.", componentMigrationName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error trying to perform migration: '{MigrationName}'.", componentMigrationName);
        }
    }
}
