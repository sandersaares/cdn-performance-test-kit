namespace Common;

public static class Constants
{
    public static readonly Uri TimeserverUrl = new Uri("ntp://time.windows.com");

    public const int MediaSegmentDurationSeconds = 2;

    /// <summary>
    /// We set this to the minimum value of 1 because even though it will be updated every 2 seconds,
    /// we would rather prefer more frequent fetches when possible because otherwise we run the risk
    /// that the CDN sees the manifest near the end of the 2 second window (e.g. at 1.9 seconds) and
    /// so will cache the manifest even though a new one is available. There may be CDN-specific workarounds
    /// but the most brute force option to at least decrease latency a bit is to just set
    /// a 1 second cache duration and accept that there will be excessive reads on the manifest files.
    /// </summary>
    public const int ManifestCacheDurationSeconds = 1;
}
