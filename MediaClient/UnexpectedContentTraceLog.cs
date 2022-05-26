using Nito.AsyncEx;
using System.Diagnostics;
using System.Text;

namespace MediaClient
{
    public sealed class UnexpectedContentTraceLog : IAsyncDisposable
    {
        /// <summary>
        /// We avoid logging too much just to avoid spamming the disk full if it starts happening too much for whatever reason.
        /// </summary>
        private static readonly TimeSpan MinIntervalBetweenReports = TimeSpan.FromSeconds(1);

        public UnexpectedContentTraceLog(
            MediaClientOptions options,
            IHostApplicationLifetime hostApplicationLifetime,
            ILogger<UnexpectedContentTraceLog> logger)
        {
            _writer = new StreamWriter(options.UnexpectedContentLogFilePath, append: false, Encoding.UTF8);
            _logger = logger;

            _cancel = hostApplicationLifetime.ApplicationStopping;
        }

        private readonly StreamWriter _writer;
        private readonly ILogger<UnexpectedContentTraceLog> _logger;
        private readonly CancellationToken _cancel;

        public ValueTask DisposeAsync()
        {
            return _writer.DisposeAsync();
        }

        private readonly AsyncLock _lock = new();
        private readonly Stopwatch _timeSinceLastReport = Stopwatch.StartNew();

        private static readonly string[] TraceIdHeaderNames = new[]
        {
            // Azure Front Door trace ID examples:
            // X-Azure-Ref: 02DJNYgAAAAATBSO+wpLgTY+U4L2DLxvrU0lOMzBFREdFMDIxNgBiYmE3ZTIxMS1hNGI1LTQ4N2EtYWIxMi02ZTNkNDVhNjY3ZTA=
            // X-Azure-Ref-OriginShield: 02DJNYgAAAABVx/0u30l4Q6qwHMmF0AfnSEtCRURHRTA3MTkAYmJhN2UyMTEtYTRiNS00ODdhLWFiMTItNmUzZDQ1YTY2N2Uw
            // X-MSEdge-Ref: ...
            // X-MS-Ref: ...
            // X-Cache: TCP_MISS
            "X-Azure-Ref",
            "X-Azure-Ref-OriginShield",
            "X-MSEdge-Ref",
            "X-MS-Ref",
            "X-Cache"
        };

        public void RecordResponseWithUnexpectedContent(string url, HttpResponseMessage response, TimeSpan contentAge)
        {
            Task.Run(async delegate
            {
                try
                {
                    using (await _lock.LockAsync(_cancel))
                    {
                        // We only record occasional samples, to not flood the log if we get suddenly overloaded with outdated reponses.
                        if (_timeSinceLastReport.Elapsed < MinIntervalBetweenReports)
                            return;

                        var traceIds = new Dictionary<string, string>();

                        foreach (var candidate in TraceIdHeaderNames)
                        {
                            if (!response.Headers.TryGetValues(candidate, out var values))
                                continue;

                            traceIds[candidate] = values.First();
                        }

                        var traceIdsString = string.Join(" - ", traceIds.Select(pair => $"{pair.Key}: {pair.Value}"));

                        await _writer!.WriteLineAsync($"{DateTimeOffset.UtcNow:O} - {url} - {contentAge.TotalSeconds:F1} seconds old - {traceIdsString}");
                        await _writer.FlushAsync();
                    }
                }
                catch (OperationCanceledException) when (_cancel.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to record trace data for outdated response: " + ex.Message);
                }
            });
        }
    }
}
