using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    internal class ExportEntitiesJob : AzureDataLakeJobBase
    {
        public ExportEntitiesJob(ApplicationContext appContext) : base(appContext)
        {
        }

        protected override async Task DoRunAsync(ExecutionContext context, JobArgs args)
        {
            var streamRepository = context.ApplicationContext.Container.Resolve<IStreamRepository>();
            var client = context.ApplicationContext.Container.Resolve<IAzureDataLakeClient>();

            var streamId = new Guid(args.Message);
            var streamModel = await streamRepository.GetStream(streamId);

            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;
            var executionContext = context.ApplicationContext.CreateExecutionContext(streamModel.OrganizationId);

            var configuration = await AzureDataLakeConnectorJobData.Create(context, providerDefinitionId, containerName);

            if (!configuration.IsStreamCacheEnabled)
            {
                context.Log.LogDebug("Stream cache is not enabled for stream {StreamId}. Skipping export.", streamModel.Id);
                return;
            }

            if (streamModel.Status != StreamStatus.Started)
            {
                context.Log.LogDebug("Stream not started for stream {StreamId}. Skipping export.", streamModel.Id);
                return;
            }

            var tableName = $"Stream_{streamId}";
            await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
            await connection.OpenAsync();

            var cronSchedule = NCrontab.CrontabSchedule.Parse(args.Schedule);
            var next = cronSchedule.GetNextOccurrence(DateTime.UtcNow.AddMinutes(1));
            var nextNext = cronSchedule.GetNextOccurrence(next.AddMinutes(1));
            var diff = nextNext - next;
            var asOfTime = next - diff;

            var command = new SqlCommand($"SELECT * FROM [{tableName}] FOR SYSTEM_TIME AS OF '{asOfTime:o}'", connection);
            command.CommandType = CommandType.Text;
            using var reader = await command.ExecuteReaderAsync();

            // TODO: Better handling of validfrom & validto
            var fieldNames = Enumerable.Range(0, reader.VisibleFieldCount).Select(reader.GetName)
                .Except(new[] { "ValidFrom", "ValidTo" }).ToList();

            var outputFormat = configuration.OutputFormat.ToLowerInvariant();
            var outputFileName = $"{streamId}_{asOfTime:o}.{outputFormat}";
            var directoryClient = await client.EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(outputFileName);
            using var outputStream = await dataLakeFileClient.OpenWriteAsync(true);

            using var loggingScope = context.Log.BeginScope(new Dictionary<string, object>
            {
                ["FileName"] = outputFileName,
                ["Format"] = outputFormat,
                ["StartTime"] = DateTimeOffset.UtcNow,
            });
            var sqlDataWriter = GetSqlDataWriter(outputFormat);
            await sqlDataWriter?.WriteAsync(context, outputStream, fieldNames, reader);
        }

        private static ISqlDataWriter GetSqlDataWriter(string outputFormat)
        {
            ISqlDataWriter sqlDataWriter = null;

            if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
            {
                sqlDataWriter = new CsvSqlDataWriter();
            }
            else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Json, StringComparison.OrdinalIgnoreCase))
            {
                sqlDataWriter = new JsonSqlDataWriter();
            }
            else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
            {
                sqlDataWriter = new ParquetSqlDataWriter();
            }

            return sqlDataWriter;
        }
    }


}
