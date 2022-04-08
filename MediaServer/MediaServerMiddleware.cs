using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Nito.AsyncEx;
using Prometheus;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace MediaServer;

/// <summary>
/// Uploads media files received from FFmpeg to Azure Blob Storage.
/// Media server applies correct metadata and caching headers, as appropriate to file type.
/// 
/// POST /files/foo/bar -> /files/XXXXX/foo/bar
/// </summary>
/// <remarks>
/// We try here to simulate a media server with a large number of media streams. To easily do that, the media stream is
/// cloned many times over, with the XXXXX in the URL being the zero-padded media stream index. If all works well, every
/// such media stream will have identical contents and identical latency.
/// 
/// Multiple instances of this app can be started to put even more stress on the storage account from multiple machines.
/// 
/// Cleanup is not implemented - files stay there forever unless cleaned up by lifecycle policy.
/// </remarks>
public sealed class MediaServerMiddleware : IAsyncDisposable
{
    private const string ContainerName = "files";
    private const string MediaStreamIndexFormatString = "D5";

    public MediaServerMiddleware(
        RequestDelegate next,
        IHostApplicationLifetime hostApplicationLifetime,
        MediaServerOptions options,
        ILogger<MediaServerMiddleware> logger)
    {
        _next = next;
        // A little questionable design here (host will not wait for us to stop like this) but good enough for a prototype.
        _cancel = hostApplicationLifetime.ApplicationStopping;
        _options = options;
        _logger = logger;

        // This is the amount of files we accept in buffer, if the actual uploading becomes a bottleneck.
        // Under normal conditions, we will never exhaust this semaphore - only during outage.
        // x2 because we have manifest+segment per media stream.
        // x5 for a very generous 10 seconds of buffer (includes all currently uploading).
        _maxInProgress = new SemaphoreSlim(_options.MaxMediaStreams * 2 * 5);

        // Note that retries are relatively expensive - they take up buffers and queue slots and will compete
        // with new (incoming) data that also needs to be uploaded. Therefore, we prefer tiny timeouts and will
        // simply skip any files that fail to upload - yes, a gap in the stream is bad but the data will not be
        // any good to us later even if it does succeed after some retries.
        var storageAccountClientOptions = new BlobClientOptions();
        
        // This also needs to include the "queued for slot in connection pool" time.
        // This can be a relatively long time (a second or two) when the pool is scaling out.
        storageAccountClientOptions.Retry.NetworkTimeout = TimeSpan.FromSeconds(3);

        // Transient errors do happen with blob storage and we can either give up or retry.
        // Neither option is deal but let's at least try to retry once, to avoid highly transient cases.
        storageAccountClientOptions.Retry.Delay = TimeSpan.FromSeconds(1);
        storageAccountClientOptions.Retry.MaxRetries = 1;

        var httpHandler = new SocketsHttpHandler
        {
            // A big spike of connections can kill network I/O even if it fits within reasonable pool size limits.
            // For the connection pool to work well, it needs to be warmed up "gently" and not all at once.
            MaxConnectionsPerServer = 512
        };

        storageAccountClientOptions.Transport = new HttpClientTransport(httpHandler);

        _storageAccountClient = new BlobServiceClient(
            _options.ConnectionString,
            storageAccountClientOptions);
        _containerClient = new AsyncLazy<BlobContainerClient>(InitializeStorageAccountAsync);
    }

    private readonly RequestDelegate _next;
    private readonly CancellationToken _cancel;
    private readonly MediaServerOptions _options;
    private readonly ILogger<MediaServerMiddleware> _logger;

    private readonly BlobServiceClient _storageAccountClient;

    // We use this to judge how many simulated media streams are connected.
    private readonly Stopwatch _timeSinceStartup = new Stopwatch();

    public ValueTask DisposeAsync()
    {
        // TODO: Not thread-safe, what happens if some async task finishes after this.
        _maxInProgress.Dispose();

        return new();
    }

    public async Task Invoke(HttpContext context)
    {
        if (context.Request.Method != "POST" || !context.Request.Path.HasValue)
        {
            await _next(context);
            return;
        }

        var requestBody = await ReadResponseBodyAsync(context);
        var filePath = context.Request.Path.Value;

        // The HTTP request handler does not care what happens with the publishing, our job here is done.
        _ = PublishFileAsync(filePath, requestBody);
    }

    private async Task<byte[]> ReadResponseBodyAsync(HttpContext context)
    {
        MemoryStream buffer;
        
        if (context.Request.ContentLength != null)
            buffer = new MemoryStream((int)context.Request.ContentLength.Value);
        else
            buffer = new MemoryStream();

        await context.Request.BodyReader.CopyToAsync(buffer, context.RequestAborted);

        return buffer.ToArray();
    }

    private readonly AsyncLazy<BlobContainerClient> _containerClient;

    private async Task<BlobContainerClient> InitializeStorageAccountAsync()
    {
        // Initializing just means making sure the container exists.
        var client = _storageAccountClient.GetBlobContainerClient(ContainerName);

        await client.CreateIfNotExistsAsync(PublicAccessType.Blob, cancellationToken: _cancel);

        return client;
    }

    // We limit how many blobs we accept for publishing in parallel.
    // If we do not have enough quota for all instances of a file, we stop right there and report a fault back to upstream.
    private readonly SemaphoreSlim _maxInProgress;

    private async Task PublishFileAsync(string originalPath, byte[] contentBytes)
    {
        try
        {
            var containerClient = await _containerClient.Task;

            string cacheControl = "max-age=60"; // Let's just be conservative while in development, to avoid old data getting stuck in caches.
            string contentType = "video/mp4";

            if (originalPath.EndsWith(".m3u8"))
            {
                cacheControl = "max-age=1";
                contentType = "application/vnd.apple.mpegurl";

                // We add a Unix timestamp in milliseconds to the end of the manifest file, for E2E latency detection. Assuming accurate clock sync here!
                var timestampLine = Encoding.UTF8.GetBytes(Environment.NewLine + "#TIME=" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                contentBytes = contentBytes.Concat(timestampLine).ToArray();
            }

            var uploadOptions = new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    CacheControl = cacheControl,
                    ContentType = contentType,
                },
                TransferOptions = new StorageTransferOptions
                {
                    MaximumConcurrency = 1
                }
            };

            var blobContent = new BinaryData(contentBytes);

            _timeSinceStartup.Start();

            var connectedMediaStreams = Math.Min(_options.MaxMediaStreams, _timeSinceStartup.Elapsed.TotalSeconds * _options.MediaStreamsPerSecond);
            ConnectedMediaStreams.Set(connectedMediaStreams);

            for (var mediaStreamIndex = _options.StartIndex; mediaStreamIndex < _options.StartIndex + connectedMediaStreams; mediaStreamIndex++)
            {
                var targetPath = mediaStreamIndex.ToString(MediaStreamIndexFormatString, CultureInfo.InvariantCulture) + originalPath;
                var blobClient = containerClient.GetBlobClient(targetPath);

                if (!_maxInProgress.Wait(TimeSpan.Zero))
                {
                    FilesThrottled.Inc();
                    return;
                }

                // Each instance forks off independently from here.
                _ = Task.Run(async delegate
                {
                    try
                    {
                        await PublishFileInstanceAsync(blobClient, blobContent, uploadOptions);
                    }
                    finally
                    {
                        _maxInProgress.Release();
                    }
                });
            }

            FilesAccepted.Inc();
        }
        catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
        {
            // This is OK - we are shutting down.
        }
        catch (Exception ex)
        {
            // This is not awaited, so we better catch and handle everything here.
            _logger.LogError(ex, $"Failed to publish file {originalPath}: {ex.Message}");
        }
    }

    private async Task PublishFileInstanceAsync(BlobClient blobClient, BinaryData content, BlobUploadOptions options)
    {
        try
        {
            BlobsInProgress.Inc();

            try
            {
                var duration = Stopwatch.StartNew();
                await blobClient.UploadAsync(content, options, _cancel);

                BlobPublishDuration.Observe(duration.Elapsed.TotalSeconds);
            }
            finally
            {
                BlobsInProgress.Dec();
            }
        }
        catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
        {
            // This is OK - we are shutting down.
        }
        catch (Exception ex)
        {
            BlobsFailed.Inc();

            ReportException(ex);

            // This is not awaited, so we better catch and handle everything here.
            _logger.LogError(ex, $"Failed to publish to {blobClient.Name}: {ex.Message}");
        }
    }

    private void ReportException(Exception ex)
    {
        if (ex is AggregateException aex)
        {
            foreach (var iex in aex.InnerExceptions)
                ReportException(iex);
        }
        else if (ex is RequestFailedException rex)
        {
            Exceptions.WithLabels(rex.GetType().Name + ":" + rex.Status).Inc();
        }
        else
        {
            Exceptions.WithLabels(ex.GetType().Name).Inc();
        }
    }

    private static readonly Counter FilesAccepted = Metrics.CreateCounter("mlms_files_accepted_total", "Total files accepted for processing.");
    private static readonly Counter FilesThrottled = Metrics.CreateCounter("mlms_files_throttled_total", "Total files throttled (either fully or partially not uploaded).");

    private static readonly Counter BlobsFailed = Metrics.CreateCounter("mlms_blobs_failed_total", "Number of blobs that failed to upload.");
    private static readonly Gauge BlobsInProgress = Metrics.CreateGauge("mlms_blobs_in_progress", "Number of blobs currently being uploaded.");

    private static readonly Gauge ConnectedMediaStreams = Metrics.CreateGauge("mlms_media_streams_connected", "Number of simulated media streams that are currently connected.");

    private static readonly Counter Exceptions = Metrics.CreateCounter(
        "mlms_blob_publish_exceptions_total",
        "Exceptions occurred during blob upload. May include multiple events from publishing of a single (retried) blob.",
        new CounterConfiguration
            {
                LabelNames = new[] { "type" }
            });

    private static readonly Histogram BlobPublishDuration = Metrics.CreateHistogram(
        "mlms_blobs_published_duration_seconds",
        "How long it took to publish each blob. Only counts successfully published blobs.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 2, 10)
        });
}