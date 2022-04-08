using Mono.Options;
using Prometheus;
using Prometheus.Experimental;
using System.Diagnostics;

namespace MediaServer;

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
        builder.Services.AddSingleton(new MediaServerOptions(StartIndex, MediaStreamCount, MediaStreamsPerSecond, StorageAccountConnectionString!));

        builder.WebHost.UseUrls($"http://+:{ListenPort}");

        var app = builder.Build();
        var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("");

        app.UseRouting();

        app.Map("/files", x => x.UseMiddleware<MediaServerMiddleware>());

        app.MapMetrics();
        logger.LogInformation($"Exposing metrics on http://<this machine>:{ListenPort}/metrics");
        logger.LogInformation($"Exposing media file storage on http://<this machine>:{ListenPort}/files");

        app.Run();
    }

    private static string? StorageAccountConnectionString;
    private static int StartIndex = 0;
    private static int MediaStreamCount = 500;

    // A slow start ensures we do not get throttled by Azure Storage with a sudden spike on startup.
    private static int MediaStreamsPerSecond = 10;

    private const ushort ListenPort = 5005;

    private static bool ParseArguments(string[] args)
    {
        var showHelp = false;
        var debugger = false;

        var options = new OptionSet
        {
            @$"Usage: MediaServer.exe [--start-index {StartIndex}] [--media-stream-count {MediaStreamCount}] --connection-string=""foo=bar""",
            "",
            { "h|?|help", "Displays usage instructions.", val => showHelp = val != null },
            "",
            { "start-index=", $"Index of the first media stream to publish from this instance. Defaults to {StartIndex}.", (int val) => StartIndex = val },
            { "media-stream-count=", $"Max number of media streams to publish from this instance. Defaults to {MediaStreamCount}.", (int val) => MediaStreamCount = val },
            { "media-streams-per-second=", $"Number of media streams to connect every second, to avoid a sudden rush at startup. Defaults to {MediaStreamsPerSecond}.", (int val) => MediaStreamsPerSecond = val },
            { "connection-string=", "Connection string for the Azure Blob Storage account that will act as the origin server for the media streams.", (string val) => StorageAccountConnectionString = val },
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

            if (StorageAccountConnectionString == null)
                throw new OptionException("Azure Storage connection string missing or invalid.", "connection-string");
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