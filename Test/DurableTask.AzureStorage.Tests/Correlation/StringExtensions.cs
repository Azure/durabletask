using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Tests.Correlation
{
    public static class StringExtensions
    {
        public static TraceParent ToTraceParent(this string traceParent)
        {
            if (!string.IsNullOrEmpty(traceParent))
            {
                var substrings = traceParent.Split('-');
                if (substrings.Length != 4)
                {
                    throw new ArgumentException($"Traceparent doesn't respect the spec. {traceParent}");
                }

                return new TraceParent
                {
                    Version = substrings[0],
                    TraceId = substrings[1],
                    SpanId = substrings[2],
                    TraceFlags = substrings[3]
                };
            }

            return null;
        }
    }

}
