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

namespace DurableTask.AzureServiceFabric.Models
{
    using DurableTask.Core;

    /// <summary>
    /// Task orchestration creation parameters.
    /// </summary>
    public class CreateTaskOrchestrationParameters
    {
        /// <summary>
        /// Task message representing task orchestration.
        /// </summary>
        public TaskMessage TaskMessage { get; set; }

        /// <summary>
        /// States of previous orchestration executions to be considered while de-duping new orchestrations on the client.
        /// </summary>
        public OrchestrationStatus[] DedupeStatuses { get; set; }
    }
}
