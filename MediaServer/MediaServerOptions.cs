namespace MediaServer;

public sealed record MediaServerOptions(int StartIndex, int MaxMediaStreams, int MediaStreamsPerSecond, string ConnectionString);