using Castle.Components.DictionaryAdapter;
using CluedIn.Connector.AzureDataLake.Helpers;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Relational;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Parquet.Schema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Extensions;

internal static class DictionaryExtension
{
    public static void AddOrUpdate<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        if (key == null)
            return;

        if (!dictionary.TryAdd(key, value))
            dictionary[key] = value;
    }

    public static TValue TryAddOrUpdate<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, Func<TValue> func)
    {
        lock (dictionary)
        {
            if (!dictionary.TryGetValue(key, out var value))
            {
                value = func();
                dictionary.AddOrUpdate(key, value);
            }

            return value;
        }
    }

    public static Stream ToJsonStream<T>(this T data)
    {
        var settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.None,
            Formatting = Formatting.Indented,
        };

        var json = JsonConvert.SerializeObject(data, settings);

        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        return stream;
    }
}
