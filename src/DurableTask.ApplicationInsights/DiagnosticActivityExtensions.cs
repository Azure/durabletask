using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

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

        private static readonly Func<Activity, (ActivityStatusCode Status, string Description)> s_getStatus;

        static DiagnosticActivityExtensions()
        {
            s_getStatus = CreateGetStatus();
        }

        public static ActivityStatusCode GetStatus(this Activity activity) => activity.GetStatus(out _);

        public static ActivityStatusCode GetStatus(this Activity activity, out string statusDescription)
        {
            ActivityStatusCode status;
            (status, statusDescription) = s_getStatus(activity);
            return status;
        }

        private static ActivityStatusCode GetOtelStatus(this Activity activity, out string statusDescription)
        {
            Debug.Assert(activity != null, "Activity should not be null");

            ActivityStatusCode status = ActivityStatusCode.Unset;
            statusDescription = null;

            foreach (var tag in activity.Tags)
            {
                switch (tag.Key)
                {
                    case "otel.status_code":
                        if (!TryGetStatusCodeForTagValue(tag.Value, out status))
                        {
                            return ActivityStatusCode.Unset;
                        }

                        break;
                    case "otel.status_description":
                        statusDescription = tag.Value;
                        break;
                    default:
                        continue;
                }
            }

            return status;
        }

        private static Func<Activity, (ActivityStatusCode Status, string Description)> CreateGetStatus()
        {
            PropertyInfo getStatus = typeof(Activity).GetProperty("Status");
            if (getStatus is null)
            {
                return activity =>
                {
                    ActivityStatusCode status = activity.GetOtelStatus(out string description);
                    return (status, description);
                };
            }

            PropertyInfo getDescription = typeof(Activity).GetProperty("StatusDescription");
            return activity =>
            {
                int s = (int)getStatus.GetValue(activity);
                string d = (string)getDescription.GetValue(activity);
                return ((ActivityStatusCode)s, d);
            };
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
