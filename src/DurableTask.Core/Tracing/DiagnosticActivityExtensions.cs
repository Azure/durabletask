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
    internal enum ActivityStatusCode
    {
        Unset = 0,
        OK = 1,
        Error = 2,
    }

    /// <summary>
    /// Extensions for <see cref="Activity"/>.
    /// </summary>
    internal static class DiagnosticActivityExtensions
    {
        private static readonly Action<Activity, string> s_setSpanId;
        private static readonly Action<Activity, string> s_setId;
        private static readonly Action<Activity, ActivityStatusCode, string> s_setStatus;

        static DiagnosticActivityExtensions()
        {
            BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
            s_setSpanId = typeof(Activity).GetField("_spanId", flags).CreateSetter<Activity, string>();
            s_setId = typeof(Activity).GetField("_id", flags).CreateSetter<Activity, string>();
            s_setStatus = CreateSetStatus();
        }

        public static void SetId(this Activity activity, string id)
            => s_setId(activity, id);

        public static void SetSpanId(this Activity activity, string spanId)
            => s_setSpanId(activity, spanId);

        public static void SetStatus(this Activity activity, ActivityStatusCode status, string description)
            => s_setStatus(activity, status, description);

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

            /*
                building expression tree to effectively perform:
                (activity, status, description) => activity.SetStatus((ActivityStatusCode)(int)status, description);
            */

            ParameterExpression targetExp = Expression.Parameter(typeof(Activity), "target");
            ParameterExpression status = Expression.Parameter(typeof(ActivityStatusCode), "status");
            ParameterExpression description = Expression.Parameter(typeof(string), "description");
            UnaryExpression convert = Expression.Convert(status, typeof(int));
            convert = Expression.Convert(convert, method.GetParameters().First().ParameterType);
            MethodCallExpression callExp = Expression.Call(targetExp, method, convert, description);
            return Expression.Lambda<Action<Activity, ActivityStatusCode, string>>(callExp, targetExp, status, description)
                .Compile();
        }
    }
}
