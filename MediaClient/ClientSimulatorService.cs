using Koek;
using Prometheus;
using System.Diagnostics;
using System.Globalization;

namespace MediaClient;

public sealed class ClientSimulatorService : IHostedService, IAsyncDisposable
{
    private static readonly TimeSpan ManifestRefreshInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SegmentProbeInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// If the manifest is at least this old, we report the trace IDs (if present) to the log file.
    /// </summary>
    private static readonly TimeSpan OutdatedManifestThreshold = TimeSpan.FromSeconds(5);

    public ClientSimulatorService(
        MediaClientOptions options,
        OutdatedContentTraceLog outdatedContentTraceLog,
        ILogger<ClientSimulatorService> logger)
    {
        _options = options;
        _outdatedContentTraceLog = outdatedContentTraceLog;
        _logger = logger;

        _cancel = _cts.Token;
    }

    private readonly MediaClientOptions _options;
    private readonly OutdatedContentTraceLog _outdatedContentTraceLog;
    private readonly ILogger<ClientSimulatorService> _logger;

    private readonly CancellationTokenSource _cts = new();
    private readonly CancellationToken _cancel;

    private Task? _task;

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        if (_task != null)
            await _task.IgnoreExceptionsAsync();

        _cts.Dispose();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var handler = new SocketsHttpHandler()
        {
            // We need to simulate a lot of potential clients, so let's keep this permissive to avoid just choking due to client-side queues.
            MaxConnectionsPerServer = 4096
        };

        var client = new HttpClient(handler);

        // Aggressive timeout, to force retries.
        client.Timeout = TimeSpan.FromSeconds(3);

        for (var mediaStreamIndex = _options.StartIndex; mediaStreamIndex < _options.StartIndex + _options.MediaStreamCount; mediaStreamIndex++)
        {
            var thisIndex = mediaStreamIndex;

            _task = Task.Run(() => ExamineManifestForeverAsync(thisIndex, client));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();

        if (_task != null)
            await _task.WaitAsync(cancellationToken);
    }

    private sealed record SegmentInfo(string Path, DateTimeOffset SeenInManifest, bool IsStartupSegment, CancellationTokenSource Cts, CancellationToken Cancel)
    {
        public DateTimeOffset? Downloaded { get; set; }
    }

    private async Task ExamineManifestForeverAsync(int mediaStreamIndex, HttpClient httpClient)
    {
        // We (re)load the manifest in a loop, forever, and examine each unique segment we see.

        // We add every segment here when we first see it, and remove it when it goes away from the manifest.
        var segments = new List<SegmentInfo>();

        var manifestUrl = string.Format(_options.UrlPattern, mediaStreamIndex, "media.m3u8");
        bool isStartup = true;

        try
        {
            while (!_cancel.IsCancellationRequested)
            {
                string manifest;
                HttpResponseMessage response;

                try
                {
                    var sw = Stopwatch.StartNew();
                    response = await httpClient.GetAsync(manifestUrl, _cancel);
                    manifest = await response.Content.ReadAsStringAsync(_cancel);
                    ManifestReadDuration.Observe(sw.Elapsed.TotalSeconds);
                }
                catch (Exception ex)
                {
                    ManifestReadExceptions.WithLabels(ex.GetType().Name).Inc();
                    goto again;
                }

                ManifestReadSuccessfully.Inc();

                // Any non-comment line in the manifest is a path to a segment.
                var segmentPaths = manifest.AsNonemptyLines().Where(x => !x.StartsWith('#')).ToList();

                var timestampLine = manifest.AsNonemptyLines().Single(x => x.StartsWith("#TIME="));
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(timestampLine.Substring(6), CultureInfo.InvariantCulture));
                var manifestAge = DateTimeOffset.UtcNow - timestamp;

                ManifestE2ELatency.Observe(manifestAge.TotalSeconds);

                if (manifestAge > OutdatedManifestThreshold)
                {
                    OutdatedFiles.Inc();
                    _outdatedContentTraceLog.RecordResponseWithOutdatedManifest(manifestUrl, response, manifestAge);
                }

                // Just to verify in console that it is still doing something.
                if (mediaStreamIndex == _options.StartIndex)
                    _logger.LogInformation($"Loaded manifest with {segmentPaths.Count} segments, E2E latency {manifestAge.TotalSeconds:F2} seconds.");

                var newSegmentPaths = segmentPaths.Except(segments.Select(x => x.Path)).ToList();
                var removedSegments = segments.Where(x => !segmentPaths.Contains(x.Path)).ToList();

                // Forget about any segments that are no longer in the manifest.
                foreach (var segment in removedSegments)
                {
                    segment.Cts.Cancel();
                    segment.Cts.Dispose();

                    segments.Remove(segment);

                    if (segment.IsStartupSegment)
                        continue;

                    SegmentsProcessed.WithLabels(segment.Downloaded.HasValue.ToString()).Inc();
                }

                // Start processing segments that have freshly appeared in the manifest.
                foreach (var path in newSegmentPaths)
                {
                    var cts = CancellationTokenSource.CreateLinkedTokenSource(_cancel);
                    var segment = new SegmentInfo(path, DateTimeOffset.UtcNow, isStartup, cts, cts.Token);
                    segments.Add(segment);

                    // Startup segments are just for decoration, we do not bother processing them as we do not know how old they are so we might have false expectations.
                    if (isStartup)
                        continue;

                    NonStartupSegmentsKnown.Inc();

                    // This forks off to its own corner and we only see the changes to SegmentInfo, never hearing from the task again.
                    _ = Task.Run(() => ExamineSegmentAsync(segment, mediaStreamIndex, httpClient));
                }

                isStartup = false;

            again:
                await Task.Delay(ManifestRefreshInterval, _cancel);
            }
        }
        catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
        {
        }
    }

    private async Task ExamineSegmentAsync(SegmentInfo segment, int mediaStreamIndex, HttpClient httpClient)
    {
        try
        {
            while (!segment.Cancel.IsCancellationRequested)
            {
                var segmentUrl = string.Format(_options.UrlPattern, mediaStreamIndex, segment.Path);
                var request = new HttpRequestMessage(HttpMethod.Head, segmentUrl);
                var response = await httpClient.SendAsync(request, segment.Cancel);

                if (response.IsSuccessStatusCode)
                {
                    segment.Downloaded = DateTimeOffset.UtcNow;
                    SegmentSeenAfter.Observe((segment.Downloaded.Value - segment.SeenInManifest).TotalSeconds);
                    break;
                }
                else
                {
                    SegmentReadNonSuccessStatuses.WithLabels(((int)response.StatusCode).ToString()).Inc();
                }

                await Task.Delay(SegmentProbeInterval, segment.Cancel);
            }
        }
        catch (OperationCanceledException) when (segment.Cancel.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            SegmentReadExceptions.WithLabels(ex.GetType().Name).Inc();
        }
    }

    private static readonly Counter ManifestReadSuccessfully = Metrics.CreateCounter("mlmc_manifest_read_total", "Number of times the manifest has been read.");

    private static readonly Counter ManifestReadExceptions = Metrics.CreateCounter(
        "mlmc_manifest_read_exceptions_total",
        "Number of exceptions occurred when trying to obtain the manifest file.",
        new CounterConfiguration
        {
            LabelNames = new[] { "type" }
        });

    private static readonly Histogram ManifestReadDuration = Metrics.CreateHistogram(
        "mlmc_manifest_read_duration_seconds",
        "How long it took to read the manifest. Successful attempts only.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 1, 10)
        });

    private static readonly Counter SegmentsProcessed = Metrics.CreateCounter(
        "mlmc_segments_total",
        "How many segments were processed by the client, and whether it had been downloaded by the time it was removed from the manifest. We do not count initial segments that exist on startup (first load).",
        new CounterConfiguration
        {
            LabelNames = new[] { "downloaded" }
        });

    private static readonly Counter SegmentReadExceptions = Metrics.CreateCounter(
        "mlmc_segment_read_exceptions_total",
        "Number of exceptions occurred when trying to obtain a segment file. Does not count expected 'failures' like 404.",
        new CounterConfiguration
        {
            LabelNames = new[] { "type" }
        });

    private static readonly Counter SegmentReadNonSuccessStatuses = Metrics.CreateCounter(
        "mlmc_segment_read_non_success_statuses_total",
        "Number of non-successful HTTP response status codes received when trying to inspect media segments.",
        new CounterConfiguration
        {
            LabelNames = new[] { "code" }
        });

    private static readonly Histogram SegmentSeenAfter = Metrics.CreateHistogram(
        "mlmc_segment_seen_after_seconds",
        "How many seconds it took for us to confirm that a segment exists at the expected URL.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 2, 10)
        });


    private static readonly Histogram ManifestE2ELatency = Metrics.CreateHistogram(
        "mlmc_manifest_e2e_latency_seconds",
        "End to end latency between the manifest being published and becoming available to the client.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 3, 10)
        });

    private static readonly Counter OutdatedFiles = Metrics.CreateCounter("mlmc_outdated_files_total", "Number of files that were out of date when downmloaded.");

    private static readonly Counter NonStartupSegmentsKnown = Metrics.CreateCounter("mlmc_known_nonstartup_segments_total", "How many non-startup (processable) segments we know about.");
}
