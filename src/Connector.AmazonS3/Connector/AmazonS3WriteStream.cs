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
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Constructor");
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
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Write");
        var shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            UploadPartAsync().GetAwaiter().GetResult();
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - WriteAsync");
        var shouldFlush = WriteInternal(buffer, offset, count);
        if (shouldFlush)
        {
            await UploadPartAsync();
        }
    }
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool WriteInternal(byte[] buffer, int offset, int count)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - WriteInternal");
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
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Flush");
        FlushAsync().GetAwaiter().GetResult();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - FlushAsync");
        if (_buffer.Length > 0)
        {
            Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - FlushAsync UploadPartAsync");
            await UploadPartAsync();
        }
    }
    [MethodImpl(MethodImplOptions.NoInlining)]
    protected override void Dispose(bool disposing)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Dispose");
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
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - DisposeAsync");
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
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - InitiateMultipartUploadAsync - Begin");
        if (_uploadId != null)
        {
            return;
        }

        var request = new InitiateMultipartUploadRequest
        {
            BucketName = _bucketName,
            Key = _key,
        };

        try
        {
            var response = await _s3Client.InitiateMultipartUploadAsync(request);
            _uploadId = response.UploadId;
            Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - InitiateMultipartUploadAsync - End" + _uploadId);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task UploadPartAsync()
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - UploadPartAsync - Begin");
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

        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - UploadPartAsync - UploadPartAsync - Begin");
        try
        {
            var response = await _s3Client.UploadPartAsync(request);
            _partETags.Add(new PartETag(_partNumber, response.ETag));
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            throw;
        }

        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - UploadPartAsync - UploadPartAsync - End");
        await sendStream.DisposeAsync();
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - UploadPartAsync - UploadPartAsync - DisposedSendStream");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task CompleteUploadAsync()
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - CompleteUploadAsync");
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
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            await AbortMultipartUploadAsync();
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task CompleteMultiPartUploadAsync()
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - CompleteMultiPartUploadAsync");
        var completeRequest = new CompleteMultipartUploadRequest
        {
            BucketName = _bucketName,
            Key = _key,
            UploadId = _uploadId,
            PartETags = _partETags,
        };

        try
        {
            await _s3Client.CompleteMultipartUploadAsync(completeRequest);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task UploadSinglePartFileAsync()
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - UploadSinglePartFileAsync");
        _buffer.Position = 0;
        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = _key,
            InputStream = _buffer,
        };

        try
        {
            await _s3Client.PutObjectAsync(putRequest);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task AbortMultipartUploadAsync()
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - AbortMultipartUploadAsync");
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
        catch (Exception ex)
        {
            // Best-effort abort; do not mask the original exception.
            Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Read");
        throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - Seek");
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        Console.WriteLine("AmazonS3WriteStream - " + this.GetHashCode() + " - SetLength");
        throw new NotSupportedException();
    }
}
