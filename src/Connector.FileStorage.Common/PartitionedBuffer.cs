using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.FileStorage.Common.Buffers;
using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common
{
    internal enum BufferStrategy
    {
        Unknown = 0,
        Legacy = 1,
        Safe = 2,
        Channel = 3,
    };

    internal class PartitionedBuffer<TPartition, TItem> : IDisposable
    {
        private readonly int _maxSize;
        private readonly int _timeout;
        private readonly Func<TPartition, TItem[], Task> _bulkAction;
        private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
        private readonly BufferStrategy _bufferStrategy;
        private readonly Dictionary<TPartition, IBuffer<TItem>> _buffers;

        public PartitionedBuffer(
            int maxSize,
            int timeout,
            Func<TPartition, TItem[], Task> bulkAction,
            IDateTimeOffsetProvider dateTimeOffsetProvider,
            BufferStrategy bufferStrategy)
        {
            _maxSize = maxSize;
            _timeout = timeout;
            _bulkAction = bulkAction ?? throw new ArgumentNullException(nameof(bulkAction));
            _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
            _bufferStrategy = bufferStrategy;
            _buffers = new Dictionary<TPartition, IBuffer<TItem>>();
        }

        public async Task Add(TPartition partition, TItem item)
        {
            IBuffer<TItem> buffer;
            lock (_buffers)
            {
                if (!_buffers.TryGetValue(partition, out buffer))
                {
                    _buffers.Add(partition, buffer = CreateBuffer(partition));
                }
            }

            await buffer.Add(item);
        }

        private IBuffer<TItem> CreateBuffer(TPartition partition)
        {
            if (_bufferStrategy == BufferStrategy.Legacy)
            {
                return new Buffer<TItem>(_maxSize, _timeout, x => _bulkAction(partition, x));
            }
            else if (_bufferStrategy == BufferStrategy.Channel)
            {
                var options = new ChannelBasedBufferOptions
                {
                    MaxBatchSize = _maxSize,
                    IdleTimeout = TimeSpan.FromMilliseconds(_timeout),
                    BoundedCapacity = 200,
                    MaxConcurrentFlushes = 1,
                    MaxPendingFlushBatches = 2,
                    EnableAutoTuneMaxBatchSize = true,
                };
                return new ChannelBasedBuffer<TItem>(options, (x, _) => _bulkAction(partition, x), _dateTimeOffsetProvider);
            }

            return new SafeBuffer<TItem, TItem>(_maxSize, _timeout, async x =>
            {
                await _bulkAction(partition, x);
                return x;
            });
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
