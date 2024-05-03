using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;


using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;

using CsvHelper;
using CsvHelper.Configuration;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using Parquet;
using Parquet.Schema;

using SixLabors.ImageSharp.ColorSpaces;

namespace CluedIn.Connector.AzureDataLake
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

            var authDetails = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configuration = new AzureDataLakeConnectorJobData(authDetails.Authentication.ToDictionary(x => x.Key, x => x.Value), containerName);


            if (!configuration.EnableBuffer)
            {
                context.Log.LogDebug("Buffer not enabled for stream {StreamId}. Skipping export.", streamModel.Id);
                return;
            }

            if (streamModel.Status != StreamStatus.Started)
            {
                context.Log.LogDebug("Stream not started for stream {StreamId}. Skipping export.", streamModel.Id);
                return;
            }

            var tableName = $"Stream_{streamId}";
            await using var connection = new SqlConnection(configuration.BufferConnectionString);
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
                .Except(new[] {"ValidFrom", "ValidTo"}).ToList();

            var outputFormat = configuration.OutputFormat.ToLowerInvariant();
            var outputFileName = $"{streamId}_{asOfTime:o}.{outputFormat}";
            var directoryClient = await client.EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(outputFileName);
            using var outputStream = await dataLakeFileClient.OpenWriteAsync(true);

            if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
            {
                await WriteCsvAsync(outputStream, fieldNames, reader);
            }
            else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Json, StringComparison.OrdinalIgnoreCase))
            {
                await WriteJsonAsync(outputStream, fieldNames, reader);
            }
            else if (outputFormat.Equals(AzureDataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
            {
                await WriteParquetAsync(outputStream, fieldNames, reader);
            }
        }

        private async Task WriteJsonAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            using var stringWriter = new StreamWriter(outputStream);
            using var writer = new JsonTextWriter(stringWriter);
            writer.Formatting = Formatting.Indented;

            await writer.WriteStartArrayAsync();
            while (await reader.ReadAsync())
            {
                await writer.WriteStartObjectAsync();

                for (int i =0; i< fieldNames.Count; i++)
                {
                    await writer.WritePropertyNameAsync(reader.GetName(i));
                    await writer.WriteValueAsync(reader.GetValue(i));
                }

                await writer.WriteEndObjectAsync();
            }
            await writer.WriteEndArrayAsync();
        }

        private async Task WriteParquetAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            var fields = new List<Field>();
            foreach (var fieldName in fieldNames)
            {
                if (fieldName == "Id")
                {
                    fields.Add(new DataField(fieldName, typeof(Guid)));
                }
                else
                {
                    fields.Add(new DataField(fieldName, typeof(string)));
                }
            }

            var schema = new ParquetSchema(fields);
            var parquetTable = new Parquet.Rows.Table(schema)
            {
            };
            using var writer = await ParquetWriter.CreateAsync(parquetTable.Schema, outputStream);
            while (await reader.ReadAsync())
            {
                var fieldValues = fieldNames.Select(reader.GetValue);
                parquetTable.Add(new Parquet.Rows.Row(fieldValues));
            }

            await writer.WriteAsync(parquetTable);
        }

        private async Task WriteCsvAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            using var writer = new StreamWriter(outputStream);

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture);
            using var csv = new CsvWriter(writer, csvConfig);
            foreach (var fieldName in fieldNames)
            {
                csv.WriteField(fieldName);
            }
            await csv.NextRecordAsync();
            while (await reader.ReadAsync())
            {
                var fieldValues = fieldNames.Select(reader.GetValue);
                foreach (var field in fieldValues)
                {
                    csv.WriteField(field);
                }
                await csv.NextRecordAsync();
            }
        }
            
    }
}
