using System;
using System.IO;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeBufferedWriteStream : Stream
{
    private const int BufferSize = 4 * 1024 * 1024; // 4MB, Azure Data Lake request size
    BufferedStream _bufferedStream;
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
}
