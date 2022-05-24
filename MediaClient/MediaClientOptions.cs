namespace MediaClient;

public sealed record MediaClientOptions(int StartIndex, int MediaStreamCount, string UrlPattern, string OutdatedContentLogFilePath, string MediaPlaylistFilename, bool EnableEtag)
{
    /// <summary>
    /// URL pattern for resolving files from Azure Storage.
    /// Parameter 0 - media stream index.
    /// Parameter 1 - filename.
    /// </summary>
    public string UrlPattern { get; init; } = UrlPattern;
}
