using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;

namespace ApplicationInsightsSample
{
    public class Processor : ITelemetryProcessor
    {
        private readonly ITelemetryProcessor _next;

        public Processor(ITelemetryProcessor next)
        {
            _next = next;
        }

        public void Process(ITelemetry item)
        {
            // Filters out storage access spans so Gantt chart is easier to read
            if (item is DependencyTelemetry d && d.Target.Contains("core.windows.net"))
            {
                return;
            }

            _next.Process(item);
        }
    }
}