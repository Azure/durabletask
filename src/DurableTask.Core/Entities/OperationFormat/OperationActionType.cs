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
#nullable enable
namespace DurableTask.Core.Entities.OperationFormat
{
    /// <summary>
    /// Enumeration of entity operation actions.
    /// </summary>
    public enum OperationActionType
    {
        /// <summary>
        /// A signal was sent to an entity
        /// </summary>
        SendSignal,

        /// <summary>
        /// A new fire-and-forget orchestration was started
        /// </summary>
        StartNewOrchestration,
    }
}