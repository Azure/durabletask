using System;
using System.Diagnostics;
using Newtonsoft.Json;

namespace DurableTask.Core
{
    /// <summary>
    /// Manage TraceContext for Dependency.
    /// This class share the TraceContext using AsyncLocal.
    /// </summary>
    public class DistributedTraceContextCorrelation
    {
        static DistributedTraceContextCorrelation()
        {
            CustomJsonSerializerSettings = new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.Objects,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
                ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
            };
        }

        /// <summary>
        /// Share the TraceContext on the call graph contextBase.
        /// </summary>
        [JsonIgnore]
        static JsonSerializerSettings CustomJsonSerializerSettings { get; }

        /// <summary>
        /// Serializable Json string of TraceContext
        /// </summary>
        [JsonIgnore]
        public string SerializableTraceContext =>
            JsonConvert.SerializeObject(this, CustomJsonSerializerSettings);

        /// <summary>
        /// Share the TraceContext on the call graph contextBase.
        /// </summary>
        public static Activity Current { get; set; }
    }
}
