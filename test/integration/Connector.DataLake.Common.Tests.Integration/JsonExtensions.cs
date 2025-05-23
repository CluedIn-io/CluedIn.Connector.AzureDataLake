using System.Text.Json.Nodes;
using System.Text.Json;

namespace CluedIn.Connector.DataLake.Common.Tests.Integration;

public static partial class JsonExtensions
{
    readonly static JsonSerializerOptions defaultOptions = new()
    {
        Converters =
        {
            AlphabeticJsonNodeConverter.Instance,
        },
        WriteIndented = true,
    };

    public static string ToAlphabeticJsonString(this JsonNode? node, JsonSerializerOptions? options = default)
    {
        if (options == null)
            options = defaultOptions;
        else
        {
            options = new JsonSerializerOptions(options);
            options.Converters.Insert(0, AlphabeticJsonNodeConverter.Instance);
        }
        return JsonSerializer.Serialize(node, options);
    }

    public static string ToAlphabeticJsonString(this string? json, JsonSerializerOptions? options = default)
    {
        if (string.IsNullOrEmpty(json))
        {
            return json;
        }

        var node = JsonSerializer.Deserialize<JsonNode>(json);

        return node.ToAlphabeticJsonString(options);
    }
}
