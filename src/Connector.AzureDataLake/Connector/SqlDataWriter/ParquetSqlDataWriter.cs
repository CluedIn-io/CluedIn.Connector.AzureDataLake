using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Data.SqlClient;

using Parquet;
using Parquet.Schema;

using ParquetTable = Parquet.Rows.Table;
using ParquetRow = Parquet.Rows.Row;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter
{
    internal class ParquetSqlDataWriter : SqlDataWriterBase
    {
        public const int Threshold = 10000;
        public override async Task WriteAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            await WriteEntireTable(context, outputStream, fieldNames, reader);
        }

        private static async Task WriteEntireTable(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            context.Log.LogDebug("Begin writing parquet file.");
            var fields = new List<Field>();
            foreach (var fieldName in fieldNames)
            {
                if (fieldName == nameof(IReadOnlyConnectorEntityData.EntityId))
                {
                    fields.Add(new DataField(fieldName, typeof(Guid)));
                }
                else
                {
                    fields.Add(new DataField(fieldName, typeof(string)));
                }
            }

            var schema = new ParquetSchema(fields);
            ParquetTable parquetTable = new ParquetTable(schema);
            using var parquetWriter = await ParquetWriter.CreateAsync(schema, outputStream);

            int i = 0;
            while (await reader.ReadAsync())
            {
                var fieldValues = fieldNames.Select(key => GetValue(key, reader));
                parquetTable.Add(new ParquetRow(fieldValues));

                i++;
                if (i % Threshold == 0)
                {
                    context.Log.LogDebug("Row group threshold {Threshold} reached. Current row {CurrentRow}. Writing data.", Threshold, i);
                    await parquetWriter.WriteAsync(parquetTable);
                    
                    parquetTable = new ParquetTable(schema);
                }
            }

            if (i % Threshold != 0)
            {
                context.Log.LogDebug("Flushing remaining data. Current row {CurrentRow}", i);
                await parquetWriter.WriteAsync(parquetTable);
            }
            context.Log.LogDebug("End writing parquet file.");
        }
    }
}
