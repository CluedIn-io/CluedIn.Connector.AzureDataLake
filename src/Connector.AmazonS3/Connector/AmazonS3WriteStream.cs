using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

namespace CluedIn.Connector.AmazonS3.Connector;

/// <summary>
/// A write-only stream that uses S3 multipart upload for large files
/// and falls back to a simple PutObject for small files (single part).
/// Each part is uploaded when the internal buffer reaches <see cref="PartSize"/> bytes.
/// The upload is completed (or aborted) on flush/close/dispose.
/// </summary>
internal class AmazonS3WriteStream : Stream
{
    private const int PartSize = 5 * 1024 * 1024; // 5 MB – S3 minimum part size
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _key;
    private MemoryStream _buffer;
    private bool _disposed;
    private readonly object _bufferLock = new();

    private string _uploadId;
    private int _partNumber;
    private readonly List<PartETag> _partETags = new();
    private bool _completed;

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
        var shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            UploadPartAsync().GetAwaiter().GetResult();
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            await UploadPartAsync();
        }
    }
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool WriteInternal(byte[] buffer, int offset, int count)
    {
        var shouldFlush = false;
        lock (_bufferLock)
        {
            _buffer.Write(buffer, offset, count);
            if (_buffer.Length >= PartSize)
            {
                shouldFlush = true;
            }
        }

        return shouldFlush;
    }

    public override void Flush()
    {
        FlushAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Length > 0)
        {
            await UploadPartAsync();
        }
    }
    [MethodImpl(MethodImplOptions.NoInlining)]
    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            try
            {
                CompleteUploadAsync().GetAwaiter().GetResult();
            }
            finally
            {
                if (_buffer != null)
                {
                    _buffer?.Dispose();
                    _buffer = null;
                }
            }
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

        try
        {
            await CompleteUploadAsync();
        }
        finally
        {
            if (_buffer != null)
            {
                await _buffer.DisposeAsync();
                _buffer = null;
            }
        }

        await base.DisposeAsync();
        _disposed = true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task InitiateMultipartUploadAsync()
    {
        if (_uploadId != null)
        {
            return;
        }

        var request = new InitiateMultipartUploadRequest
        {
            BucketName = _bucketName,
            Key = _key,
        };

        var response = await _s3Client.InitiateMultipartUploadAsync(request);
        _uploadId = response.UploadId;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task UploadPartAsync()
    {
        MemoryStream sendStream;
        lock (_bufferLock)
        {
            if (_buffer == null || _buffer.Length == 0)
            {
                return;
            }

            sendStream = _buffer;
            _buffer = new MemoryStream();
        }

        await InitiateMultipartUploadAsync();

        sendStream.Position = 0;
        _partNumber++;

        var request = new UploadPartRequest
        {
            BucketName = _bucketName,
            Key = _key,
            UploadId = _uploadId,
            PartNumber = _partNumber,
            InputStream = sendStream,
        };

        var response = await _s3Client.UploadPartAsync(request);
        _partETags.Add(new PartETag(_partNumber, response.ETag));

        await sendStream.DisposeAsync();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task CompleteUploadAsync()
    {
        if (_completed)
        {
            return;
        }

        _completed = true;

        if (_buffer == null || (_buffer.Length == 0 && _uploadId == null))
        {
            return;
        }

        // No multipart upload was started – use simple PutObject for small files.
        if (_uploadId == null)
        {
            await UploadSinglePartFileAsync();
            return;
        }

        // Upload remaining buffered data as the final part.
        if (_buffer.Length > 0)
        {
            await UploadPartAsync();
        }

        try
        {
            await CompleteMultiPartUploadAsync();
        }
        catch
        {
            await AbortMultipartUploadAsync();
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task CompleteMultiPartUploadAsync()
    {
        var completeRequest = new CompleteMultipartUploadRequest
        {
            BucketName = _bucketName,
            Key = _key,
            UploadId = _uploadId,
            PartETags = _partETags,
        };

        await _s3Client.CompleteMultipartUploadAsync(completeRequest);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task UploadSinglePartFileAsync()
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

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task AbortMultipartUploadAsync()
    {
        if (_uploadId == null)
        {
            return;
        }

        try
        {
            await _s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = _key,
                UploadId = _uploadId,
            });
        }
        catch
        {
            // Best-effort abort; do not mask the original exception.
        }
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
