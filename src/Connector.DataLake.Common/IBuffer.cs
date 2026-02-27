using System;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common
{
    internal interface IBuffer<T> : IDisposable
    {
        Task Add(T item);
        Task Flush();
    }
}
