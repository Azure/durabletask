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
            if (item is DependencyTelemetry d && d.Target.StartsWith("127.0.0.1"))
            {
                return;
            }

            if (item is RequestTelemetry request && request.Name.StartsWith("orchestration"))
            {

            }

            _next.Process(item);
        }
    }
}