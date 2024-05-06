using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;

using Parquet;
using Parquet.Schema;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter
{
    internal class ParquetSqlDataWriter : SqlDataWriterBase
    {
        public override async Task WriteAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
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
                var fieldValues = fieldNames.Select(key => GetValue(key, reader));
                parquetTable.Add(new Parquet.Rows.Row(fieldValues));
            }

            await writer.WriteAsync(parquetTable);
        }
    }
}
