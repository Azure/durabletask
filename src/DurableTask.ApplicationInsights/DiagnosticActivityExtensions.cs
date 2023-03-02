using System;
using System.Diagnostics;

namespace DurableTask.ApplicationInsights
{
    internal enum ActivityStatusCode
    {
        Unset = 0,
        OK = 1,
        Error = 2,
    }

    internal static class DiagnosticActivityExtensions
    {
        private static readonly string UnsetStatusCodeTagValue = "UNSET";
        private static readonly string OkStatusCodeTagValue = "OK";
        private static readonly string ErrorStatusCodeTagValue = "ERROR";

        private static Func<Activity, ActivityStatusCode> s_getStatus;

        static DiagnosticActivityExtensions()
        {
            s_getStatus = CreateGetStatus;
        }

        public static ActivityStatusCode GetStatus(this Activity activity)
            => s_getStatus(activity);

        public static bool TryGetStatus(this Activity activity, out ActivityStatusCode statusCode, out string statusDescription)
        {
            Debug.Assert(activity != null, "Activity should not be null");

            bool foundStatusCode = false;
            statusCode = default;
            statusDescription = null;

            foreach (var tag in activity.Tags)
            {
                switch (tag.Key)
                {
                    case "otel.status_code":
                        foundStatusCode = TryGetStatusCodeForTagValue(tag.Value as string, out statusCode);
                        if (!foundStatusCode)
                        {
                            // If status code was found but turned out to be invalid give up immediately.
                            return false;
                        }

                        break;
                    case "otel.status_description":
                        statusDescription = tag.Value as string;
                        break;
                    default:
                        continue;
                }

                if (foundStatusCode && statusDescription != null)
                {
                    // If we found a status code and a description we break enumeration because our work is done.
                    break;
                }
            }

            return foundStatusCode;
        }

        private static ActivityStatusCode CreateGetStatus(Activity activity)
        {
            return !activity.TryGetStatus(out ActivityStatusCode statusCode, out string statusDescription)
                    ? ActivityStatusCode.Unset
                    : statusCode;
        }

        private static bool TryGetStatusCodeForTagValue(string statusCodeTagValue, out ActivityStatusCode statusCode)
        {
            ActivityStatusCode? tempStatusCode = GetStatusCodeForTagValue(statusCodeTagValue);

            statusCode = tempStatusCode ?? default;

            return tempStatusCode.HasValue;
        }
        private static ActivityStatusCode? GetStatusCodeForTagValue(string statusCodeTagValue)
        {
            return statusCodeTagValue switch
            {
                /*
                 * Note: Order here does matter for perf. Unset is
                 * first because assumption is most spans will be
                 * Unset, then Error. Ok is not set by the SDK.
                 */
                string _ when UnsetStatusCodeTagValue.Equals(statusCodeTagValue, StringComparison.OrdinalIgnoreCase) => ActivityStatusCode.Unset,
                string _ when ErrorStatusCodeTagValue.Equals(statusCodeTagValue, StringComparison.OrdinalIgnoreCase) => ActivityStatusCode.Error,
                string _ when OkStatusCodeTagValue.Equals(statusCodeTagValue, StringComparison.OrdinalIgnoreCase) => ActivityStatusCode.OK,
                _ => null,
            };
        }
    }
}
