using System;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common
{
    internal interface IBuffer<T> : IDisposable
    {
        Task Add(T item);
        Task Flush();
    }
}
