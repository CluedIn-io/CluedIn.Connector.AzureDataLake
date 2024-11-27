using System;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common;

internal interface IDataMigrator
{
    Task MigrateAsync();
    Task<bool> IsMigrationPerformedAsync(Guid organizationId, string key);
    Task SetMigrationPerformedAsync(Guid organizationId, string key);
}
