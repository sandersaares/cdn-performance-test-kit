using Common;
using Koek;
using Mono.Options;
using Prometheus;
using Prometheus.Experimental;
using System.Diagnostics;

namespace MediaClient;

public static class Program
{
    public static void Main(string[] args)
    {
        Metrics.SuppressDefaultMetrics();
        LocalTimeMetrics.Register();

        if (!ParseArguments(args))
        {
            Environment.ExitCode = -1;
            return;
        }

        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSingleton(new MediaClientOptions(StartIndex, MediaStreamCount, UrlPattern!, OutdatedContentLogFilePath, MediaPlaylistFilename));
        builder.Services.AddHttpClient();

        builder.Services.AddSingleton<OutdatedContentTraceLog>();
        builder.Services.AddSingleton<ITimeSource>(s => new NtpTimeSource(Constants.TimeserverUrl, s.GetRequiredService<ILogger<NtpTimeSource>>()));

        builder.Services.AddHostedService<ClientSimulatorService>();

        builder.WebHost.UseUrls($"http://+:{ListenPort}");

        var app = builder.Build();
        var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("");

        app.UseRouting();

        app.MapMetrics();

        logger.LogInformation($"Exposing metrics on http://<this machine>:{ListenPort}/metrics");

        NtpTimeMetrics.Register(app.Services.GetRequiredService<ITimeSource>());

        app.Run();
    }

    private static int StartIndex = 0;
    private static int MediaStreamCount = 1000;
    private static string? UrlPattern;
    private static ushort ListenPort = 5010;
    private static string OutdatedContentLogFilePath = "outdated-content.log";
    private static string MediaPlaylistFilename = "media.m3u8";

    private static bool ParseArguments(string[] args)
    {
        var showHelp = false;
        var debugger = false;

        var options = new OptionSet
        {
            @$"Usage: MediaClient.exe --start-index {StartIndex} --media-stream-count {MediaStreamCount}",
            "",
            { "h|?|help", "Displays usage instructions.", val => showHelp = val != null },
            "",
            { "url-pattern=", $"URL pattern to use for generating requests. Expecting a format string with parameter 0 being the media stream index and parameter 1 being the file path. Example: https://storage-account-name.blob.core.windows.net/files/{{0:D5}}/hls/{{1}}", (string val) => UrlPattern = val },
            { "start-index=", $"Index of the first media stream to consume from this instance.", (int val) => StartIndex = val },
            { "media-stream-count=", $"Number of media streams to consume from this instance.", (int val) => MediaStreamCount = val },
            { "media-playlist-filename=", $"Filename of the media playlist to read. Only single-playlist media streams are supported. Defaults to {MediaPlaylistFilename}.", (string val) => MediaPlaylistFilename = val },
            { "listen-port=", $"Port number to expose metrics on. Defaults to {ListenPort}.", (ushort val) => ListenPort = val },
            { "outdated-content-log=", $"Path to a file where to log sampled trace data from outdated content that is encountered. Defaults to {OutdatedContentLogFilePath}.", (string val) => OutdatedContentLogFilePath = val },
            "",
            { "debugger", "Requests a debugger to be attached before the app starts.", val => debugger = val != default, true }
        };

        List<string> remainingOptions;

        try
        {
            remainingOptions = options.Parse(args);

            if (args.Length == 0 || showHelp)
            {
                options.WriteOptionDescriptions(Console.Error);
                return false;
            }

            if (string.IsNullOrWhiteSpace(UrlPattern))
                throw new OptionException("URL pattern must be specified.", "url-pattern");
        }
        catch (OptionException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine("For usage instructions, use the --help command line parameter.");
            return false;
        }

        if (remainingOptions.Count != 0)
        {
            Console.Error.WriteLine("Unknown command line parameters: {0}", string.Join(" ", remainingOptions.ToArray()));
            Console.Error.WriteLine("For usage instructions, use the --help command line parameter.");
            return false;
        }

        if (debugger)
            Debugger.Launch();

        return true;

    }
}