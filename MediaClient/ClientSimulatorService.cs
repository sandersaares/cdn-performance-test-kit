using Common;
using Koek;
using Prometheus;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;

namespace MediaClient;

public sealed class ClientSimulatorService : IHostedService, IAsyncDisposable
{
    /// <summary>
    /// We refresh the manifest more often than needed, to ensure that we do not penalize the results
    /// too much due to the time difference between manifest uploads and manifest downloads, caused by the step size.
    /// </summary>
    private static readonly TimeSpan ManifestRefreshInterval = TimeSpan.FromSeconds(0.1);

    /// <summary>
    /// Once we see a segment referenced in the manifest, we probe for its existence at this rate.
    /// We expect the files to appear very fast once they are listed in the manifest, so this iterates very fast.
    /// </summary>
    private static readonly TimeSpan SegmentProbeInterval = TimeSpan.FromSeconds(0.1);

    /// <summary>
    /// If a retrieved file is at least this old, we report the response's CDN trace IDs (if present) to the outdated content log file.
    /// </summary>
    private static readonly TimeSpan OutdatedFileThreshold = TimeSpan.FromSeconds(10);

    public ClientSimulatorService(
        MediaClientOptions options,
        UnexpectedContentTraceLog outdatedContentTraceLog,
        ITimeSource timeSource,
        ILogger<ClientSimulatorService> logger)
    {
        _options = options;
        _unexpectedContentTraceLog = outdatedContentTraceLog;
        _timeSource = timeSource;
        _logger = logger;

        _cancel = _cts.Token;
    }

    private readonly MediaClientOptions _options;
    private readonly UnexpectedContentTraceLog _unexpectedContentTraceLog;
    private readonly ITimeSource _timeSource;
    private readonly ILogger<ClientSimulatorService> _logger;

    private readonly CancellationTokenSource _cts = new();
    private readonly CancellationToken _cancel;

    private ConcurrentBag<Task> _examineMediaStreamTasks = new();

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        while (_examineMediaStreamTasks.Count != 0)
        {
            if (_examineMediaStreamTasks.TryTake(out var task))
                await task.IgnoreExceptionsAsync();
        }

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

            _examineMediaStreamTasks.Add(Task.Run(() => ExamineManifestForeverAsync(thisIndex, client)));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();

        while (_examineMediaStreamTasks.Count != 0)
        {
            if (_examineMediaStreamTasks.TryTake(out var task))
                await task.IgnoreExceptionsAsync().WaitAsync(cancellationToken);
        }
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

        var manifestUrl = string.Format(_options.UrlPattern, mediaStreamIndex, _options.MediaPlaylistFilename);
        bool isStartup = true;

        // We save the etag of the manifest here, so we can short-circuit and skip any loads when it has not changed.
        string? etag = null;

        var lastUpdated = new Stopwatch();

        // It can be that the CDN does not return 304 Not Modified in response to ETag match.
        // We use this to add an additional layer of equality checking - if the timestamp is exactly the same,
        // we consider it the exact same manifest and ignore the update.
        DateTimeOffset? lastSeenTimestamp = null;

        // We use this to request only newer manifest versions.
        // Not every CDN edge node has the latest version, so this avoids rollback to undead manifests.
        DateTimeOffset? lastModified = null;

        try
        {
            while (!_cancel.IsCancellationRequested)
            {
                string manifest;
                HttpResponseMessage response;

                try
                {
                    var sw = Stopwatch.StartNew();

                    var request = new HttpRequestMessage(HttpMethod.Get, manifestUrl);

                    if (etag != null && _options.EnableEtag)
                        request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue(etag, isWeak: false));

                    if (lastModified != null)
                        request.Headers.IfModifiedSince = lastModified;

                    response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseContentRead, _cancel);

                    if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
                    {
                        // We have already seen this version of the manifest. Just loop again.
                        ManifestAlreadySeen.Inc();
                        goto again;
                    }
                    else if (!response.IsSuccessStatusCode)
                    {
                        ManifestReadNonSuccessStatuses.WithLabels(((int)response.StatusCode).ToString()).Inc();
                        goto again;
                    }

                    etag = response.Headers.ETag?.Tag;

                    manifest = await response.Content.ReadAsStringAsync(_cancel);
                    ManifestReadDuration.Observe(sw.Elapsed.TotalSeconds);

                    if (lastUpdated.IsRunning)
                        ManifestUpdateInterval.Observe(lastUpdated.Elapsed.TotalSeconds);

                    lastUpdated.Restart();
                }
                catch (HttpRequestException ex)
                {
                    ManifestReadExceptions.WithLabels($"{ex.GetType().Name} {ex.StatusCode}").Inc();
                    goto again;
                }
                catch (Exception ex)
                {
                    ManifestReadExceptions.WithLabels(ex.GetType().Name).Inc();
                    goto again;
                }

                // Any non-comment line in the manifest is a path to a segment.
                var segmentPaths = manifest.AsNonemptyLines().Where(x => !x.StartsWith('#')).ToList();

                var timestampLine = manifest.AsNonemptyLines().SingleOrDefault(x => x.StartsWith("#TIME="));

                if (timestampLine == null)
                {
                    // If an upload is aborted, it can be that there is no timestamp line.
                    // Alternatively, this can mean that the file is not generated by MediaServer, so we simply cannot measure E2E latency.
                    ManifestReadExceptions.WithLabels("No MediaServer timestamp found").Inc();
                }
                else
                {
                    var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(timestampLine.Substring(6), CultureInfo.InvariantCulture));

                    var age = _timeSource.GetCurrentTime() - timestamp;

                    if (lastSeenTimestamp == timestamp)
                    {
                        // This should have been prevent by ETag match.
                        // We assume that we can never have two different manifests with the same timestamp on the same URL.
                        ManifestReadExceptions.WithLabels("Expected 304 got 200").Inc();
                        goto again;
                    }

                    if (timestamp < lastSeenTimestamp)
                    {
                        // It is undead! The manifest is older than the previous manifest we had!
                        UndeadManifestAge.Observe(age.TotalSeconds);

                        // Report it! We report undead manifest age as the amount of time it reverted us by, not as actual age from creation.
                        // This will be a negative amount, to highlight that we went back in time.
                        var undeadManifestTimeRegression = timestamp - lastSeenTimestamp.Value;

                        _unexpectedContentTraceLog.RecordResponseWithUnexpectedContent(manifestUrl, response, undeadManifestTimeRegression);

                        // We ignore these, as they mess up our accounting.
                        // Not so sure if players will be happy with them...
                        goto again;
                    }

                    lastSeenTimestamp = timestamp;

                    ManifestAge.Observe(age.TotalSeconds);

                    if (age > OutdatedFileThreshold)
                    {
                        OutdatedFiles.Inc();
                        _unexpectedContentTraceLog.RecordResponseWithUnexpectedContent(manifestUrl, response, age);
                    }

                    // Just to verify in console that it is still doing something.
                    if (mediaStreamIndex == _options.StartIndex)
                        _logger.LogInformation($"Loaded manifest with {segmentPaths.Count} segments, manifest age {age.TotalSeconds:F2} seconds, ETag {etag}.");
                }

                ManifestReadSuccessfully.Inc();

                lastModified = response.Content.Headers.LastModified;
                
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
                    var segment = new SegmentInfo(path, _timeSource.GetCurrentTime(), isStartup, cts, cts.Token);
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
        finally
        {
            foreach (var segment in segments)
            {
                segment.Cts.Cancel();
                segment.Cts.Dispose();
            }

            segments.Clear();
        }
    }

    private async Task ExamineSegmentAsync(SegmentInfo segment, int mediaStreamIndex, HttpClient httpClient)
    {
        try
        {
            while (!segment.Cancel.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                var segmentUrl = string.Format(_options.UrlPattern, mediaStreamIndex, segment.Path);
                var response = await httpClient.GetAsync(segmentUrl, HttpCompletionOption.ResponseHeadersRead, segment.Cancel);

                if (response.IsSuccessStatusCode)
                {
                    segment.Downloaded = DateTimeOffset.UtcNow;
                    SegmentSeenAfter.Observe((segment.Downloaded.Value - segment.SeenInManifest).TotalSeconds);

                    var content = await response.Content.ReadAsByteArrayAsync();

                    SegmentReadDuration.Observe(sw.Elapsed.TotalSeconds);

                    SegmentDownloadedAfter.Observe((segment.Downloaded.Value - segment.SeenInManifest).TotalSeconds);

                    // Get the timestamp from the file. It is at the end of the file.
                    // Note that there is no guarantee that segments are produced by MediaServer - the timestamp may be missing.

                    var timestampBoxBytes = content.AsMemory(content.Length - TimestampBox.Length, TimestampBox.Length);

                    if (TimestampBox.ExistsIn(timestampBoxBytes.Span))
                    {
                        var timestampBox = TimestampBox.Deserialize(timestampBoxBytes.Span);

                        var age = _timeSource.GetCurrentTime() - timestampBox.Timestamp;
                        var ageAtDownloadStart = segment.SeenInManifest - timestampBox.Timestamp;

                        SegmentAge.Observe(age.TotalSeconds);
                        SegmentAgeAtDownloadStart.Observe(ageAtDownloadStart.TotalSeconds);

                        if (age > OutdatedFileThreshold)
                        {
                            OutdatedFiles.Inc();
                            _unexpectedContentTraceLog.RecordResponseWithUnexpectedContent(segmentUrl, response, age);
                        }
                    }

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
        catch (HttpRequestException ex)
        {
            SegmentReadExceptions.WithLabels($"{ex.GetType().Name} {ex.StatusCode}").Inc();
        }
        catch (Exception ex)
        {
            SegmentReadExceptions.WithLabels(ex.GetType().Name).Inc();
        }
    }

    private static readonly Counter ManifestReadSuccessfully = Metrics.CreateCounter("mlmc_manifest_read_total", "Number of times the manifest has been read. Only counts the first time a specific version of the manifest is seen.");

    private static readonly Counter ManifestAlreadySeen = Metrics.CreateCounter("mlmc_manifest_already_seen_total", "Number of times the manifest has been seen but proved to be a version we had already processed.");

    private static readonly Counter ManifestReadExceptions = Metrics.CreateCounter(
        "mlmc_manifest_read_exceptions_total",
        "Number of exceptions occurred when trying to obtain the manifest file.",
        new CounterConfiguration
        {
            LabelNames = new[] { "type" }
        });

    private static readonly Counter ManifestReadNonSuccessStatuses = Metrics.CreateCounter(
        "mlmc_manifest_read_non_success_statuses_total",
        "Number of non-successful HTTP response status codes received when trying to fetch manifests.",
        new CounterConfiguration
        {
            LabelNames = new[] { "code" }
        });

    private static readonly Histogram ManifestReadDuration = Metrics.CreateHistogram(
        "mlmc_manifest_read_duration_seconds",
        "How long it took to read the manifest. Successful attempts only. First read of each manifest version only.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 1, 10)
        });

    private static readonly Histogram ManifestUpdateInterval = Metrics.CreateHistogram(
        "mlmc_manifest_update_interval_seconds",
        "Wall clock time elapsed between two consecutive versions of a manifest.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 2, 10)
        });

    private static readonly Histogram SegmentReadDuration = Metrics.CreateHistogram(
        "mlmc_segment_read_duration_seconds",
        "How long it took to read a segment. Successful attempts only. Single attempt only (e.g. 404s will reset the timer).",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 1, 10)
        });

    private static readonly Histogram SegmentAgeAtDownloadStart = Metrics.CreateHistogram(
        "mlmc_segment_age_at_download_start_seconds",
        "Segment age when we first learned of it and started downloading it.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 2, 10)
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

    private static readonly Histogram SegmentDownloadedAfter = Metrics.CreateHistogram(
        "mlmc_segment_downloaded_after_seconds",
        "How many seconds it took for us to fully download a segment after we found that it exists at the expected URL.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 2, 10)
        });


    private static readonly Histogram ManifestAge = Metrics.CreateHistogram(
        "mlmc_manifest_age_seconds",
        "Age of the manifest - the time between when it was published and when it was downloaded by the client.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 3, 10)
        });

    private static readonly Histogram UndeadManifestAge = Metrics.CreateHistogram(
        "mlmc_undead_manifest_age_seconds",
        "Age of an undead manifest - when time runs backwards and we encounter a manifest from the past.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 3, 10)
        });

    private static readonly Histogram SegmentAge = Metrics.CreateHistogram(
        "mlmc_segment_age_seconds",
        "Age of the segment - total time elapsed between when it was published and when it was completely downloaded by the client.",
        new HistogramConfiguration
        {
            Buckets = Histogram.PowersOfTenDividedBuckets(-1, 3, 10)
        });

    private static readonly Counter OutdatedFiles = Metrics.CreateCounter("mlmc_outdated_files_total", "Number of files that were out of date when downmloaded.");

    private static readonly Counter NonStartupSegmentsKnown = Metrics.CreateCounter("mlmc_known_nonstartup_segments_total", "How many non-startup (processable) segments we know about.");
}
