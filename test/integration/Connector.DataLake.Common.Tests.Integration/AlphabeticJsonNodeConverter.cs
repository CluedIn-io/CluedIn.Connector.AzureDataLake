using System.Text.Json.Nodes;
using System.Text.Json;
using System;
using System.Text.Json.Serialization;
using System.Linq;

namespace CluedIn.Connector.DataLake.Common.Tests.Integration;

public class AlphabeticJsonNodeConverter : JsonConverter<JsonNode>
{
    public static AlphabeticJsonNodeConverter Instance { get; } = new AlphabeticJsonNodeConverter();

    public override bool CanConvert(Type typeToConvert) => typeof(JsonNode).IsAssignableFrom(typeToConvert) && typeToConvert != typeof(JsonValue);

    public override void Write(Utf8JsonWriter writer, JsonNode? value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case JsonObject obj:
                writer.WriteStartObject();
                foreach (var pair in obj.OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(pair.Key);
                    Write(writer, pair.Value, options);
                }
                writer.WriteEndObject();
                break;
            case JsonArray array: // We need to handle JsonArray explicitly to ensure that objects inside arrays are alphabetized
                writer.WriteStartArray();
                foreach (var item in array)
                    Write(writer, item, options);
                writer.WriteEndArray();
                break;
            case null:
                writer.WriteNullValue();
                break;
            default: // JsonValue
                value.WriteTo(writer, options);
                break;
        }
    }

    public override JsonNode? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => JsonNode.Parse(ref reader);
}
