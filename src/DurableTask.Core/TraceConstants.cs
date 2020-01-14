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
    /// <summary>
    /// TraceConstants for Distributed Tracing
    /// </summary>
    public static class TraceConstants
    {
        /// <summary>
        /// Client is the Distributed Tracing message for OrchestratorClient.
        /// </summary>
        public const string Client = "DtClient";

        /// <summary>
        /// Orchestrator is the Distributed Tracing message for Orchestrator.
        /// </summary>
        public const string Orchestrator = "DtOrchestrator";

        /// <summary>
        /// Activity is the Distributed Tracing message for Activity
        /// </summary>
        public const string Activity = "DtActivity";

        /// <summary>
        /// DependencyDefault is the Distributed Tracing message default name for Dependency.
        /// </summary>
        public const string DependencyDefault = "outbound";
    }
}
