using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

namespace CluedIn.Connector.AmazonS3.Connector;

/// <summary>
/// A write-only stream that buffers all writes in memory and uploads
/// the complete content to S3 on flush/close/dispose.
/// </summary>
internal class AmazonS3WriteStream : Stream
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _key;
    private readonly MemoryStream _buffer;
    private bool _disposed;

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
        _buffer.Write(buffer, offset, count);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _buffer.WriteAsync(buffer, offset, count, cancellationToken);
    }

    public override void Flush()
    {
        // Don't upload on every flush to avoid excessive uploads
        Aaa();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        // Don't upload on every flush to avoid excessive uploads
    }

    public void Aaa()
    {
        if (_disposed)
        {
            throw new Exception("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD disposed");
        }
    }

    protected override void Dispose(bool disposing)
    {
        //throw new NotSupportedException("sssssssssssssDispose is not supported. Use DisposeAsync instead.");
        if (!_disposed && disposing)
        {
            _disposed = true;
            UploadAsync().GetAwaiter().GetResult();
            _buffer.Dispose();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        //throw new Exception("DKDKDKDKKDKDKD DisposeAsync is not supported. Use FlushAsync instead.");
        if (!_disposed)
        {
            _disposed = true;
            await UploadAsync();
            await _buffer.DisposeAsync();
        }

        await base.DisposeAsync();
    }

    private async Task UploadAsync()
    {
        _buffer.Position = 0;
        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = _key,
            InputStream = _buffer,
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
