using System;
using System.Diagnostics;
using System.Linq;
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

        private static readonly Func<Activity, StatusTuple> s_getStatus;

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

        private static Func<Activity, StatusTuple> CreateGetStatus()
        {
            PropertyInfo getStatus = typeof(Activity).GetProperty("Status");
            if (getStatus is null)
            {
                return activity =>
                {
                    ActivityStatusCode status = activity.GetOtelStatus(out string description);
                    return new StatusTuple(status, description);
                };
            }

            /*
                building expression tree to effectively perform:
                activity => new StatusTuple((ActivityStatusCode)(int)activity.Status, activity.StatusDescription);
            */

            ConstructorInfo ctor = typeof(StatusTuple).GetConstructors().First(x => x.GetParameters().Length == 2);
            PropertyInfo getDescription = typeof(Activity).GetProperty("StatusDescription");
            ParameterExpression activity = Expression.Parameter(typeof(Activity), "activity");
            MemberExpression status = Expression.Property(activity, getStatus);
            MemberExpression description = Expression.Property(activity, getDescription);
            UnaryExpression convert = Expression.Convert(status, typeof(int));
            convert = Expression.Convert(convert, typeof(ActivityStatusCode));
            NewExpression result = Expression.New(ctor, convert, description);
            return Expression.Lambda<Func<Activity, StatusTuple>>(result, activity).Compile();
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

        /// <summary>
        /// Expression trees do not yet support ValueTuple, so we need to make one manually.
        /// </summary>
        private readonly struct StatusTuple
        {
            public StatusTuple(ActivityStatusCode status, string description)
            {
                this.Status = status;
                this.Description = description;
            }

            public ActivityStatusCode Status { get; }

            public string Description { get; }

            public void Deconstruct(out ActivityStatusCode status, out string description)
            {
                status = this.Status;
                description = this.Description;
            }
        }
    }
}
