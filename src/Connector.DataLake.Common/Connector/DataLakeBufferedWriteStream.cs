using System;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeBufferedWriteStream : Stream, IDisposable, IAsyncDisposable
{
    private const int BufferSize = 4 * 1024 * 1024; // 4MB, Azure Data Lake request size
    BufferedStream _bufferedStream;
    private bool _disposedValue;

    public DataLakeBufferedWriteStream(Stream backingStream)
    {
        if (backingStream is null)
        {
            throw new ArgumentNullException(nameof(backingStream));
        }

        _bufferedStream = new BufferedStream(backingStream, BufferSize);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => _bufferedStream.CanWrite;

    public override long Length => _bufferedStream.Length;

    public override long Position { get => _bufferedStream.Position; set => _bufferedStream.Position = value; }

    public override void Flush()
    {
        // don't flush underlying stream to prevent excessive flushes
    }

    public override void Close()
    {
        _bufferedStream.Close();
        base.Close();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Reading is not supported");
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException("Seeking is not supported");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException("Setting length is not supported.");
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _bufferedStream.Write(buffer, offset, count);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _bufferedStream?.Dispose();
            }

            _disposedValue = true;
        }
    }

    void IDisposable.Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        await _bufferedStream.DisposeAsync();
    }
}
