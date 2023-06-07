//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.ApplicationInsights
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    internal enum ActivityStatusCode
    {
        Unset = 0,
        OK = 1,
        Error = 2,
    }

    internal static class DiagnosticActivityExtensions
    {
        private const string UnsetStatusCodeTagValue = "UNSET";
        private const string OkStatusCodeTagValue = "OK";
        private const string ErrorStatusCodeTagValue = "ERROR";

        private static readonly Func<Activity, StatusTuple> s_getStatus = CreateGetStatus();

        public static ActivityStatusCode GetStatus(this Activity activity) => activity.GetStatus(out _);

        public static ActivityStatusCode GetStatus(this Activity activity, out string statusDescription)
        {
            ActivityStatusCode status;
            (status, statusDescription) = s_getStatus(activity);
            return status;
        }

        // Extension method that gets an Activity's status code and status description using tags.
        // This extension method is necessary since we depend on System.Diagnostics.DiagnosticSource 5.0.1.
        // Versions greater than 6.0.0 can directly access Activity.Status.
        private static ActivityStatusCode GetOtelStatus(this Activity activity, out string statusDescription)
        {
            ActivityStatusCode status = ActivityStatusCode.Unset;
            statusDescription = null;

            Debug.Assert(activity != null, "Activity should not be null");

            if (activity == null)
            {
                return status;
            }

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
                }
            }

            return status;
        }

        private static Func<Activity, StatusTuple> CreateGetStatus()
        {
            // The Status and StatusDescription fields are only available for apps that load System.Diagnostics.DiagnosticSource/6.0.0 or greater.
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
