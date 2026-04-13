using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using Nest;

namespace CluedIn.Connector.AmazonS3.Connector;

/// <summary>
/// A write-only stream that buffers all writes in memory and uploads
/// the complete content to S3 on flush/close/dispose.
/// </summary>
internal class AmazonS3WriteStream : Stream
{
    private const int MaxBufferSize = 4 * 1024 * 1024;
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _key;
    private MemoryStream _buffer;
    private bool _disposed;
    private readonly object _bufferLock = new();

    public AmazonS3WriteStream(IAmazonS3 s3Client, string bucketName, string key)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _buffer = new MemoryStream();
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !_disposed;
    public override long Length => _buffer.Length;
    public override long Position
    {
        get => _buffer.Position;
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        bool shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            UploadAsync().GetAwaiter().GetResult();
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            await UploadAsync();
        }
    }

    private bool WriteInternal(byte[] buffer, int offset, int count)
    {
        bool shouldFlush = false;
        lock (_bufferLock)
        {
            _buffer.Write(buffer, offset, count);
            if (_buffer.Length >= MaxBufferSize) // flush if buffer exceeds 4MB
            {
                shouldFlush = true;
            }
        }

        return shouldFlush;
    }

    public override void Flush()
    {
        FlushAsync().GetAwaiter().GetResult();
    }

    public override void Close()
    {
        base.Close();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await UploadAsync();
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _disposed = true;
            _buffer.Dispose();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _buffer.DisposeAsync();
        }

        await base.DisposeAsync();
    }

    private async Task UploadAsync()
    {
        if (_buffer.Length == 0)
        {
            return; // nothing to upload
        }

        var sendStream = _buffer;
        lock (_bufferLock)
        {
            _buffer = new MemoryStream();
        }

        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = _key,
            InputStream = sendStream,
        };

        await _s3Client.PutObjectAsync(putRequest);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }
}
