using CluedIn.Connector.AzureDataLake.Extensions;
using CluedIn.Core.Connectors;
using Parquet;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Helpers;

public static class ParquetHelper
{
    public static ParquetSchema GenerateSchema(Dictionary<string, object> datas,
        IReadOnlyCollection<ConnectorPropertyData> properties)
    {
        var fields = new List<Field>();

        foreach (var data in datas)
        {
            var property = properties.SingleOrDefault(s => s.Name == data.Key);
            var type = data.Value.GetType();
            if (property != null)
            {
                if (property.DataType.GetType() == typeof(VocabularyKeyConnectorPropertyDataType))
                {
                    type = ((VocabularyKeyConnectorPropertyDataType)property.DataType)
                        .VocabularyKey.DataType.ConvertToType();
                }
                //else if (property.DataType.GetType() == typeof(SerializableVocabularyKeyConnectorPropertyDataType))
                //{

                //}
                else if (property.DataType.GetType() == typeof(EntityPropertyConnectorPropertyDataType))
                {
                    type = ((EntityPropertyConnectorPropertyDataType)property.DataType).Type;
                }
                else if (property.DataType.GetType() == typeof(VocabularyKeyDataTypeConnectorPropertyDataType))
                {
                    type = ((VocabularyKeyDataTypeConnectorPropertyDataType)property.DataType)
                        .VocabularyKeyDataType.ConvertToType();
                }
            }

            Field field;
            field = new DataField(data.Key, type);

            //if (data.Value.GetType() != type)
            //    datas[data.Key] = data.Value.ToString();

            fields.Add(field);
        }

        return new ParquetSchema(fields);
    }

    public static async Task<Stream> ToStream(Table parquetTable)
    {
        var stream = new MemoryStream();

        using (var writer = await ParquetWriter.CreateAsync(parquetTable.Schema, stream))
        {
            await writer.WriteAsync(parquetTable);
        }

        stream.Position = 0;

        return stream;
    }

    public static async Task<Stream> ToStream(ParquetSchema parquetSchema, IReadOnlyCollection<IDictionary<string, object>> parquetData)
    {
        using var stream = new MemoryStream();
        await ParquetSerializer.SerializeAsync(parquetSchema, parquetData, stream);

        return stream;
    }
}
