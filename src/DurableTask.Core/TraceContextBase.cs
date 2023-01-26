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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using DurableTask.Core.Common;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// TraceContext keep the correlation value.
    /// </summary>
    public abstract class TraceContextBase
    {
        private static readonly JsonSerializer serializer;

        /// <summary>
        /// Default constructor 
        /// </summary>
        protected TraceContextBase()
        {
            OrchestrationTraceContexts = new Stack<TraceContextBase>();
        }

        static TraceContextBase()
        {
            CustomJsonSerializerSettings = new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.Objects,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
                ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
            };

            serializer = JsonSerializer.Create(CustomJsonSerializerSettings);   
        }

        /// <summary>
        /// Start time of this telemetry
        /// </summary>
        public DateTimeOffset StartTime { get; set; }

        /// <summary>
        /// Type of this telemetry.
        /// Request Telemetry or Dependency Telemetry.
        /// Use
        /// <see cref="TelemetryType"/> 
        /// </summary>
        public TelemetryType TelemetryType { get; set; }

        /// <summary>
        /// OrchestrationState save the state of the 
        /// </summary>
        public Stack<TraceContextBase> OrchestrationTraceContexts { get; set; }

        /// <summary>
        /// Keep OperationName in case, don't have an Activity in this context
        /// </summary>
        public string OperationName { get; set; }

        /// <summary>
        /// Current Activity only managed by this concrete class.
        /// This property is not serialized.
        /// </summary>
        [JsonIgnore]
        internal Activity CurrentActivity { get; set; }

        /// <summary>
        /// Return if the orchestration is on replay
        /// </summary>
        /// <returns></returns>
        [JsonIgnore]
        public bool IsReplay { get; set; } = false;

        /// <summary>
        /// Duration of this context. Valid after call Stop() method.
        /// </summary>
        [JsonIgnore]
        public abstract TimeSpan Duration { get; }

        [JsonIgnore]
        static JsonSerializerSettings CustomJsonSerializerSettings { get; }


        /// <summary>
        /// Serializable Json string of TraceContext
        /// </summary>
        [JsonIgnore]
        public string SerializableTraceContext =>
            Utils.SerializeToJson(serializer, this);

        /// <summary>
        /// Telemetry.Id Used for sending telemetry. refer this URL
        /// https://docs.microsoft.com/en-us/dotnet/api/microsoft.applicationinsights.extensibility.implementation.operationtelemetry?view=azure-dotnet
        /// </summary>
        [JsonIgnore]
        public abstract string TelemetryId { get; }

        /// <summary>
        /// Telemetry.Context.Operation.Id Used for sending telemetry refer this URL
        /// https://docs.microsoft.com/en-us/dotnet/api/microsoft.applicationinsights.extensibility.implementation.operationtelemetry?view=azure-dotnet
        /// </summary>
        [JsonIgnore]
        public abstract string TelemetryContextOperationId { get; }

        /// <summary>
        /// Get RequestTraceContext of Current Orchestration
        /// </summary>
        /// <returns></returns>
        public TraceContextBase GetCurrentOrchestrationRequestTraceContext()
        {
            foreach(TraceContextBase element in OrchestrationTraceContexts)
            {
                if (TelemetryType.Request == element.TelemetryType) return element;
            }

            throw new InvalidOperationException("Can not find RequestTraceContext");
        }

        /// <summary>
        /// Telemetry.Context.Operation.ParentId Used for sending telemetry refer this URL
        /// https://docs.microsoft.com/en-us/dotnet/api/microsoft.applicationinsights.extensibility.implementation.operationtelemetry?view=azure-dotnet
        /// </summary>
        [JsonIgnore]
        public abstract string TelemetryContextOperationParentId { get; }

        /// <summary>
        /// Set Parent TraceContext and Start the context
        /// </summary>
        /// <param name="parentTraceContext"> Parent Trace</param>
        public abstract void SetParentAndStart(TraceContextBase parentTraceContext);

        /// <summary>
        /// Start TraceContext as new
        /// </summary>
        public abstract void StartAsNew();

        /// <summary>
        /// Stop TraceContext
        /// </summary>
        public void Stop() => CurrentActivity?.Stop();

        /// <summary>
        /// Set Activity.Current to CurrentActivity
        /// </summary>
        public void SetActivityToCurrent()
        {
            Activity.Current = CurrentActivity;
        }

        /// <summary>
        /// Restore TraceContext sub class
        /// </summary>
        /// <param name="json">Serialized json of TraceContext sub classes</param>
        /// <returns></returns>
        public static TraceContextBase Restore(string json)
        {
            // If the JSON is empty, we assume to have an empty context
            if (string.IsNullOrEmpty(json))
            {
                return TraceContextFactory.Empty;
            }

            // Obtain typename and validate that it is a subclass of `TraceContextBase`.
            // If it's not, we throw an exception.
            Type traceContextType = null;
            Type traceContextBasetype = typeof(TraceContextBase);

            JToken typeName = JObject.Parse(json)["$type"];
            traceContextType = Type.GetType(typeName.Value<string>());
            if (!traceContextType.IsSubclassOf(traceContextBasetype))
            {
                string typeNameStr = typeName.ToString();
                string baseNameStr = traceContextBasetype.ToString();
                throw new Exception($"Serialized TraceContext type ${typeNameStr} is not a subclass of ${baseNameStr}." +
                    "This probably means something went wrong in serializing the TraceContext.");
            }

            // De-serialize the object now that we now it's safe
            var restored = Utils.DeserializeFromJson(
                serializer,
                json,
                traceContextType) as TraceContextBase;
            restored.OrchestrationTraceContexts = new Stack<TraceContextBase>(restored.OrchestrationTraceContexts);
            return restored;
        }
    }

    /// <summary>
    /// Telemetry Type
    /// </summary>
    public enum TelemetryType
    {
        /// <summary>
        /// Request Telemetry
        /// </summary>
        Request,

        /// <summary>
        /// Dependency Telemetry
        /// </summary>
        Dependency,
    }
}
