using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal class FileStorageBufferedWriteStream : Stream
{
    private bool _disposed;
    private readonly BufferedStream _bufferedStream;
    public FileStorageBufferedWriteStream(Stream backingStream, int bufferSize)
    {
        if (backingStream is null)
        {
            throw new ArgumentNullException(nameof(backingStream));
        }

        _bufferedStream = new BufferedStream(backingStream, bufferSize);
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

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        // don't flush underlying stream to prevent excessive flushes
        return Task.CompletedTask;
    }

    public override void Close()
    {
        _bufferedStream.Flush();
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
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _bufferedStream?.Dispose();
        }

        base.Dispose(disposing);
        _disposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (!_disposed)
        {
            if (_bufferedStream != null)
            {
                await _bufferedStream.DisposeAsync();
            }
        }

        await base.DisposeAsync();
        _disposed = true;
    }
}
