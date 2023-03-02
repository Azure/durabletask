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

namespace DurableTask.Core.Tracing
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Replica from System.Diagnostics.DiagnosticSource >= 6.0.0
    /// </summary>
    public enum ActivityStatusCode
    {
        ///<summary>Unset</summary>
        Unset = 0,
        ///<summary>OK</summary>
        OK = 1,
        ///<summary>Error</summary>
        Error = 2,
    }

    /// <summary>
    /// Extensions for <see cref="Activity"/>.
    /// </summary>
    public static class DiagnosticActivityExtensions
    {
        private static readonly string UnsetStatusCodeTagValue = "UNSET";
        private static readonly string OkStatusCodeTagValue = "OK";
        private static readonly string ErrorStatusCodeTagValue = "ERROR";

        private static readonly Action<Activity, string> s_setSpanId;
        private static readonly Action<Activity, string> s_setId;
        private static readonly Action<Activity, ActivityStatusCode, string> s_setStatus;
        private static Func<Activity, ActivityStatusCode> s_getStatus;

        static DiagnosticActivityExtensions()
        {
            BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
            s_setSpanId = typeof(Activity).GetField("_spanId", flags).CreateSetter<Activity, string>();
            s_setId = typeof(Activity).GetField("_id", flags).CreateSetter<Activity, string>();
            s_setStatus = CreateSetStatus();
            s_getStatus = CreateGetStatus;
        }

        /// <summary>
        /// Set ID
        /// </summary>
        /// <param name="activity"></param>
        /// <param name="id"></param>
        public static void SetId(this Activity activity, string id)
            => s_setId(activity, id);

        /// <summary>
        /// Set Span ID
        /// </summary>
        /// <param name="activity"></param>
        /// <param name="spanId"></param>
        public static void SetSpanId(this Activity activity, string spanId)
            => s_setSpanId(activity, spanId);

        /// <summary>
        /// Set Status
        /// </summary>
        /// <param name="activity"></param>
        /// <param name="status"></param>
        /// <param name="description"></param>
        public static void SetStatus(this Activity activity, ActivityStatusCode status, string description)
            => s_setStatus(activity, status, description);

        /// <summary>
        /// Get Status
        /// </summary>
        /// <param name="activity"></param>
        public static ActivityStatusCode GetStatus(this Activity activity)
            => s_getStatus(activity);

        private static Action<Activity, ActivityStatusCode, string> CreateSetStatus()
        {
            MethodInfo method = typeof(Activity).GetMethod("SetStatus");
            if (method is null)
            {
                return (activity, status, description) => {
                    if (activity is null)
                    {
                        throw new ArgumentNullException(nameof(activity));
                    }
                    string str = status switch
                    {
                        ActivityStatusCode.Unset => "UNSET",
                        ActivityStatusCode.OK => "OK",
                        ActivityStatusCode.Error => "ERROR",
                        _ => null,
                    };
                    activity.SetTag("otel.status_code", str);
                    activity.SetTag("otel.status_description", description);
                };
            }
            ParameterExpression targetExp = Expression.Parameter(typeof(Activity), "target");
            ParameterExpression status = Expression.Parameter(typeof(ActivityStatusCode), "status");
            ParameterExpression description = Expression.Parameter(typeof(string), "description");
            UnaryExpression convert = Expression.Convert(status, typeof(int));
            convert = Expression.Convert(convert, method.GetParameters().First().ParameterType);
            MethodCallExpression callExp = Expression.Call(targetExp, method, convert, description);
            return Expression.Lambda<Action<Activity, ActivityStatusCode, string>>(callExp, targetExp, status, description)
                .Compile();
        }

        /// <summary>
        /// Create Get Status
        /// </summary>
        /// <param name="activity"></param>
        /// <returns></returns>
        public static ActivityStatusCode CreateGetStatus(Activity activity)
        {
            return !activity.TryGetStatus(out ActivityStatusCode statusCode, out string statusDescription)
                    ? ActivityStatusCode.Unset
                    : statusCode;
        }

        /// <summary>
        /// Try Get Status
        /// </summary>
        /// <param name="activity"></param>
        /// <param name="statusCode"></param>
        /// <param name="statusDescription"></param>
        /// <returns></returns>
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
                    case Schema.Status.Code:
                        foundStatusCode = TryGetStatusCodeForTagValue(tag.Value as string, out statusCode);
                        if (!foundStatusCode)
                        {
                            // If status code was found but turned out to be invalid give up immediately.
                            return false;
                        }

                        break;
                    case Schema.Status.Description:
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

        /// <summary>
        /// Try Get Status Code
        /// </summary>
        /// <param name="statusCodeTagValue"></param>
        /// <param name="statusCode"></param>
        /// <returns></returns>
        public static bool TryGetStatusCodeForTagValue(string statusCodeTagValue, out ActivityStatusCode statusCode)
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
