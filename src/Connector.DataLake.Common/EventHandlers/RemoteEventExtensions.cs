using System;

using CluedIn.Core;
using CluedIn.Core.Events;

using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal static class RemoteEventExtensions
{
    internal static bool TryGetResourceInfo(this RemoteEvent remoteEvent, string organizationIdKey, string resourceIdKey, out Guid organizationId, out Guid resourceId)
    {
        if (!(remoteEvent.EventData?.StartsWith("{") ?? false))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        var eventObject = JsonUtility.Deserialize<JObject>(remoteEvent.EventData);
        if (!eventObject.TryGetValue(organizationIdKey, out var accountIdToken))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        if (!eventObject.TryGetValue(resourceIdKey, out var resourceIdToken))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        var accountId = accountIdToken.Value<string>();
        var resourceIdString = resourceIdToken.Value<string>();
        resourceId = new Guid(resourceIdString);
        organizationId = new Guid(accountId);

        return true;

        static void setResultToDefault(out Guid organizationId, out Guid resourceId)
        {
            organizationId = Guid.Empty;
            resourceId = Guid.Empty;
        }
    }
}
