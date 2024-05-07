using Newtonsoft.Json;
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace CluedIn.Connector.AzureDataLake.Helpers;

public static class StreamHelper
{
    public static string ConvertToStreamString(object data)
    {
        var binFormatter = new BinaryFormatter();
        using var mStream = new MemoryStream();
        binFormatter.Serialize(mStream, data);

        return Convert.ToBase64String(mStream.ToArray());
    }

    public static T ConvertToObject<T>(string streamString)
    {
        var bytes = Convert.FromBase64String(streamString);
        var mStream = new MemoryStream(bytes);

        var binFormatter = new BinaryFormatter();
        return (T)binFormatter.Deserialize(mStream);
    }
}
