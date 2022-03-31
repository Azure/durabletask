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

using System;
using System.Diagnostics;
using System.Threading;
using Newtonsoft.Json;

namespace DurableTask.Core.Tracing
{
    /// <summary>
    /// Manage Activity for orchestration execution.
    /// </summary>
    internal class DistributedTraceActivity
    {
        private static AsyncLocal<Activity> CurrentActivity = new AsyncLocal<Activity>();

        private static readonly JsonSerializerSettings CustomJsonSerializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Objects,
            PreserveReferencesHandling = PreserveReferencesHandling.Objects,
            ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
        };

        /// <summary>
        /// Share the Activity across an orchestration execution.
        /// </summary>
        internal static Activity Current
        {
            get { return CurrentActivity.Value; }
            set { CurrentActivity.Value = value; }
        }

        /// <summary>
        /// Serialize the orchestration Activity
        /// </summary>
        internal static string SerializeActivity(DistributedTraceContext distributedTraceContext, Activity activity)
        {
            SerializedActivity serializedActivity = new SerializedActivity();
            serializedActivity.DistributedTraceContext = distributedTraceContext;
            serializedActivity.SpanId = activity.SpanId.ToString();
            serializedActivity.StartTime = activity.StartTimeUtc;
            return JsonConvert.SerializeObject(serializedActivity, CustomJsonSerializerSettings);
        }

        /// <summary>
        /// Restore Activity sub class
        /// </summary>
        /// <param name="json">Serialized Activity json</param>
        /// <returns></returns>
        internal static SerializedActivity Restore(string json)
        {
            // If the JSON is empty, we assume to have an empty context
            if (string.IsNullOrEmpty(json))
            {
                return null;
            }

            // De-serialize the object now that we now it's safe
            SerializedActivity restored = JsonConvert.DeserializeObject<SerializedActivity>(json, CustomJsonSerializerSettings);

            return restored;
        }
    }

    /// <summary>
    /// Manage serialized Activity
    /// </summary>
    internal class SerializedActivity
    {
        /// <summary>
        /// DistributedTraceContext which stores the traceparent and tracestate
        /// </summary>
        public DistributedTraceContext DistributedTraceContext { get; set; }

        /// <summary>
        /// Stores the SpanId for the orchestration Activity.
        /// </summary>
        public string SpanId { get; set; }

        /// <summary>
        /// Stores the start time for the orchestration Activity.
        /// </summary>
        public DateTime StartTime { get; set; }
    }
}
