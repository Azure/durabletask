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
    using System.Text;

    /// <summary>
    /// HttpCorrelationProtocolTraceContext keep the correlation value with HTTP Correlation Protocol
    /// </summary>
    public class HttpCorrelationProtocolTraceContext : TraceContextBase
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public HttpCorrelationProtocolTraceContext() : base() { }

        /// <summary>
        /// ParentId for backward compatibility
        /// </summary>
        public string ParentId { get; set; }

        /// <summary>
        /// ParentId for parent
        /// </summary>
        public string ParentParentId { get; set; }

        /// <inheritdoc />
        public override void SetParentAndStart(TraceContextBase parentTraceContext)
        {
            CurrentActivity = new Activity(this.OperationName);

            if (parentTraceContext is HttpCorrelationProtocolTraceContext)
            {
                var context = (HttpCorrelationProtocolTraceContext)parentTraceContext;
                CurrentActivity.SetParentId(context.ParentId); // TODO check if it is context.ParentId or context.CurrentActivity.Id 
                OrchestrationTraceContexts = context.OrchestrationTraceContexts.Clone();
            }

            CurrentActivity.Start();

            ParentId = CurrentActivity.Id;
            StartTime = CurrentActivity.StartTimeUtc;
            ParentParentId = CurrentActivity.ParentId;

            CorrelationTraceContext.Current = this;
        }

        /// <inheritdoc />
        public override void StartAsNew()
        {
            CurrentActivity = new Activity(this.OperationName);
            CurrentActivity.Start();

            ParentId = CurrentActivity.Id;
            StartTime = CurrentActivity.StartTimeUtc;
            ParentParentId = CurrentActivity.ParentId;

            CorrelationTraceContext.Current = this;
        }

        /// <inheritdoc />
        public override TimeSpan Duration => CurrentActivity?.Duration ?? DateTimeOffset.UtcNow - StartTime;

        /// <inheritdoc />
        public override string TelemetryId => CurrentActivity?.Id ?? ParentId;

        /// <inheritdoc />
        public override string TelemetryContextOperationId => CurrentActivity?.RootId ?? GetRootId(ParentId);

        /// <inheritdoc />
        public override string TelemetryContextOperationParentId => CurrentActivity?.ParentId ?? ParentParentId;

        // internal use. Make it internal for testability.
        internal string GetRootId(string id) =>  id?.Split('.').FirstOrDefault()?.Replace("|", "");        
    }
}
