using CluedIn.Connector.AzureDataLake.Extensions;
using CluedIn.Core.Connectors;
using Parquet;
using Parquet.File.Values.Primitives;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Helpers;

public static class ParquetHelper
{
    public static readonly Type[] SupportedTypes = new[] {
            typeof(bool),
            typeof(byte), typeof(sbyte),
            typeof(short), typeof(ushort),
            typeof(int), typeof(uint),
            typeof(long), typeof(ulong),
            typeof(float),
            typeof(double),
            typeof(decimal),
            typeof(BigInteger),
            typeof(DateTime),
#if NET6_0_OR_GREATER
            typeof(DateOnly),
            typeof(TimeOnly),
#endif
            typeof(TimeSpan),
            typeof(Interval),
            typeof(byte[]),
            typeof(string),
            typeof(Guid)
        };

    public static ParquetSchema GenerateSchema(Dictionary<string, object> datas,
        IReadOnlyCollection<ConnectorPropertyData> properties)
    {
        var fields = new List<Field>();

        foreach (var data in datas)
        {
            var property = properties.SingleOrDefault(s => s.Name == data.Key);
            var type = data.Value?.GetType() ?? typeof(string);
            var typeInfo = type.GetTypeInfo();
            //var args = typeInfo.GenericTypeArguments;

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

            if (!SupportedTypes.Contains(type))
                type = typeof(string);

            datas[data.Key] = ValidateData(type, data.Value);

            var field = new DataField(data.Key, type);
            
            fields.Add(field);
        }

        return new ParquetSchema(fields);
    }

    public static object ValidateData(Type type, object value)
    {
        if (value == null)
            return string.Empty;
        else if (value.GetType() != type)
            return value.ToString();
        else if (type == typeof(Uri))
            return (value as Uri)?.ToString() ?? string.Empty;

        return value;
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
