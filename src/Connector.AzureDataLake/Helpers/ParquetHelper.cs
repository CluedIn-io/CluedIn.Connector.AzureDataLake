using CluedIn.Connector.AzureDataLake.Extensions;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using Parquet;
using Parquet.File.Values.Primitives;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading.Tasks;
using IEntityUri = CluedIn.Core.Data.Parts.IEntityUri;

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

    public static void ExtractTypeAndData(Dictionary<string, object> datas,
        IReadOnlyCollection<ConnectorPropertyData> properties, out Dictionary<string, Type> fieldMap)
    {
        fieldMap = new();
        foreach (var data in datas)
        {
            var type = data.Value?.GetType();
            var dataValue = data.Value;
            var property = properties.SingleOrDefault(s => s.Name == data.Key);

            if (property != null)
                type = property.GetDataType();

            var isArray = TryExtractIEnumerableType(type, out var baseType);

            if ((!SupportedTypes.Contains(type.GetNonNullable()) && !isArray) || (baseType != null && !SupportedTypes.Contains(baseType.GetNonNullable())))
            {
                if (type == typeof(PersonReference[]))
                {
                    type = typeof(string[]);
                    dataValue = (data.Value as PersonReference[]).Select(s => s?.ToString()).ToArray();
                }
                else if (type == typeof(DateTimeOffset?) || type == typeof(DateTimeOffset))
                {
                    if (type.IsNullableType())
                    {
                        dataValue = data.Value == null ? (DateTime?)null : (data.Value as DateTimeOffset?).Value.DateTime;
                        type = typeof(DateTime?);
                    }
                    else
                    {
                        dataValue = ((DateTimeOffset)data.Value).DateTime;
                        type = typeof(DateTime);
                    }
                }
                else if (type == typeof(EntityType))
                {
                    type = typeof(string);
                    dataValue = data.Value == null ? (string)null : JsonUtility.Serialize(data.Value);
                }
                else if (type == typeof(CultureInfo))
                {
                    type = typeof(string);
                    dataValue = (string)(data.Value as CultureInfo)?.ToString();
                }
                else if (type == typeof(Uri))
                {
                    type = typeof(string);
                    dataValue = (string)(data.Value as Uri)?.AbsoluteUri;
                }
                else if (type == typeof(PersonReference))
                {
                    type = typeof(string);
                    dataValue = (data.Value as PersonReference)?.ToString();
                }
                else if (type == typeof(IEntityUri[]))
                {
                    type = typeof(string[]);
                    dataValue = (data.Value as IEntityUri[])?.Select(s => s.ToString()).ToArray();
                }
                else if (type == typeof(ITag[]))
                {
                    type = typeof(string[]);
                    dataValue = (data.Value as ITag[])?.Select(s => s.ToString()).ToArray();
                }
                else if (type == typeof(EntityEdge[]))
                {
                    type = typeof(string[]);
                    dataValue = (data.Value as EntityEdge[]).Select(s => s.ToString()).ToArray();
                }
                else if (type == typeof(IEntityCode))
                {

                }
                else
                    throw new NotImplementedException(type.Name);
            }

            datas[data.Key] = dataValue;

            if (isArray && !type.IsArray)
                type = type.MakeArrayType();

            fieldMap.Add(data.Key, type);
        }
    }

    public static bool TryExtractIEnumerableType(Type type, out Type? baseType)
    {

        var ti = type.GetTypeInfo();
        var args = ti.GenericTypeArguments;

        if (args.Length > 0)
        {
            //check derived interfaces
            foreach (var interfaceType in ti.ImplementedInterfaces)
            {
                var iti = interfaceType.GetTypeInfo();
                if (iti.IsGenericType && iti.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    baseType = iti.GenericTypeArguments[0];
                    return true;
                }
            }

            //check if this is an IEnumerable<>
            if (ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                baseType = ti.GenericTypeArguments[0];
                return true;
            }
        }

        if (ti.IsArray)
        {
            baseType = ti.GetElementType();
            return true;
        }

        baseType = null;
        return false;
    }

    public static ParquetSchema GenerateSchema(Dictionary<string, object> datas,
        IReadOnlyCollection<ConnectorPropertyData> properties)
    {
        var fields = new List<Field>();

        ExtractTypeAndData(datas, properties, out var fieldMap);
        return new ParquetSchema(fieldMap.Select(s => new DataField(s.Key, s.Value)));
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
