using CluedIn.Core.Configuration;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.Common.Caching
{
    public class SqlServerCachingService<TItem, TConfiguration> : ICachingService<TItem, TConfiguration>
    {
        private readonly string _connectionString;
        private readonly string _primaryConnectionStringKeyName = "Streams.Common.SqlCacheConnectionString";
        private readonly string _fallbackConnectionStringKeyName = "CLUEDIN_CONNECTIONSTRINGS__CLUEDINENTITIES";

        private readonly string _tableName = "Streams.SqlCaching";
        private readonly string _configurationColumn = "Configuration";
        private readonly string _dataColumn = "Data";        

        private SqlServerCachingService()
        {
            var connectionStringSettings = ConfigurationManagerEx.ConnectionStrings[_primaryConnectionStringKeyName] ??
                ConfigurationManagerEx.ConnectionStrings[_fallbackConnectionStringKeyName];
            _connectionString = connectionStringSettings.ConnectionString;
        }

        public static async Task<SqlServerCachingService<TItem,TConfiguration>> CreateCachingService()
        {
            var service = new SqlServerCachingService<TItem, TConfiguration>();
            await service.EnsureTableCreated();

            return service;
        }

        public async Task AddItem(TItem item, TConfiguration configuration)
        {
            Log.Information("SqlServerCachingService.AddItem: entry");
            var serializedData = JsonConvert.SerializeObject(item);
            var serializedConfiguration = JsonConvert.SerializeObject(configuration);

            var query = $@"INSERT INTO {_tableName} ({_dataColumn}, {_configurationColumn})
                        VALUES ('{serializedData}', '{serializedConfiguration}');";

            await ExecuteNonQuery(query);
            Log.Information("SqlServerCachingService.AddItem: exit");
        }

        public async Task Clear()
        {
            Log.Information("SqlServerCachingService.Clear: entry");
            var query = $"DELETE FROM {_tableName}";
            await ExecuteNonQuery(query);
            Log.Information("SqlServerCachingService.Clear: exit");
        }

        public async Task Clear(TConfiguration configuration)
        {
            Log.Information("SqlServerCachingService.Clear(config): entry");
            var serializedConfiguration = JsonConvert.SerializeObject(configuration);
            var query = $"DELETE FROM {_tableName} WHERE {_configurationColumn}='{serializedConfiguration}'";
            await ExecuteNonQuery(query);
            Log.Information("SqlServerCachingService.Clear(config): exit");
        }

        public async Task<int> Count()
        {
            Log.Information("SqlServerCachingService.Count: entry");
            var query = $@"SELECT COUNT({_dataColumn}) FROM {_tableName}";
            using var connection = new SqlConnection(_connectionString);
            var command = new SqlCommand(query, connection);
            await command.Connection.OpenAsync();

            Log.Information("SqlServerCachingService.Count: exit");
            return (int) await command.ExecuteScalarAsync();            
        }

        public Task<IQueryable<KeyValuePair<TItem, TConfiguration>>> GetItems()
        {
            throw new NotImplementedException();
        }

        private async Task<int> ExecuteNonQuery(string query)
        {
            using var connection = new SqlConnection(_connectionString);
            var command = new SqlCommand(query, connection);
            await command.Connection.OpenAsync();

            return await command.ExecuteNonQueryAsync();
        }        

        private async Task EnsureTableCreated()
        {
            Log.Information("SqlServerCachingService.EnsureTableCreated: entry");
            var query = @$"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{_tableName}' and xtype='U')
                                    CREATE TABLE {_tableName} (
                                    {_dataColumn} varchar(max) not null,
                                    {_configurationColumn} varchar(max) not null
                                    )
                                GO";

            await ExecuteNonQuery(query);            
            Log.Information("SqlServerCachingService.EnsureTableCreated: table created");
        }
    }
}
