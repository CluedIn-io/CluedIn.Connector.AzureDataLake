using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common;

internal interface IBuffer<T> : IDisposable
{
    Task Add(T item);
    Task Flush();
    Task<BufferStatus> GetStatus();
}

public record BufferStatus(
    int TotalPendingItems,
    int MaxPendingItems,
    int TimeOutMilliseconds,
    Dictionary<string, string> AdditionalInformation);
