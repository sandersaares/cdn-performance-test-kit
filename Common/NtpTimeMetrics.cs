using Koek;
using Prometheus;

namespace Common;

public static class NtpTimeMetrics
{
    public static void Register(ITimeSource timeSource)
    {
        var time = Metrics.CreateGauge(
            "ntptime_seconds",
            "Current time synchronized regularly against an NTP time source.",
            new GaugeConfiguration
            {
                SuppressInitialValue = true
            });

        Metrics.DefaultRegistry.AddBeforeCollectCallback(delegate
        {
            time.SetToTimeUtc(timeSource.GetCurrentTime());
        });
    }
}
