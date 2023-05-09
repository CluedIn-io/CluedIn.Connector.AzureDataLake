using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake
{
    internal class PartitionedBuffer<TItem> : IDisposable
    {
        private readonly int _maxSize;
        private readonly int _timeout;
        private readonly Action<TItem[]> _bulkAction;
        private readonly Dictionary<object, Buffer<TItem>> _buffers;

        public PartitionedBuffer(int maxSize, int timeout, Action<TItem[]> bulkAction)
        {
            _maxSize = maxSize;
            _timeout = timeout;
            _bulkAction = bulkAction;
            _buffers = new Dictionary<object, Buffer<TItem>>();
        }

        public async Task Add(TItem item, object partition)
        {
            Buffer<TItem> buffer;
            lock (_buffers)
            {
                if (!_buffers.TryGetValue(partition, out buffer))
                {
                    _buffers.Add(partition, buffer = new Buffer<TItem>(_maxSize, _timeout, _bulkAction));
                }
            }

            await buffer.Add(item);
        }

        public void Dispose()
        {
            foreach (var buffer in _buffers)
            {
                buffer.Value.Dispose();
            }
        }

        public async Task Flush()
        {
            foreach (var buffer in _buffers)
            {
                await buffer.Value.Flush();
            }
        }
    }
}
